package org.apache.flink.streaming.controlplane.rescale.streamswitch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.controlplane.rescale.controller.OperatorControllerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ConfigurableDummyStreamSwitch implements FlinkOperatorController, Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(ConfigurableDummyStreamSwitch.class);

	private final String name;

	private OperatorControllerListener listener;

	Map<String, List<String>> executorMapping;

	private volatile boolean waitForMigrationDeployed;

	private volatile boolean isStopped;

	private Random random;

	private List<RescaleActionDescriptor.BaseRescaleAction> actionList;

	public ConfigurableDummyStreamSwitch() {
		this("ConfigurableDummyStreamSwitch");
	}

	public ConfigurableDummyStreamSwitch(String name) {
		this.name = name;
	}

	private void setRescaleAction(List<RescaleActionDescriptor.BaseRescaleAction> actionList) {
		this.actionList = actionList;
	}

	@Override
	public synchronized void start() {
		new Thread(this).start();
	}

	/**
	 * If the task described by this jobVertex should be rescaled, the name should be in this format,
	 * "rescale: `(ACTION1, ACTION2, ...)`"
	 * <p>
	 * The ACTION could have several options:
	 * 1. out: newParallelism
	 * 2. in: newParallelism
	 * 3. repartition
	 * 4. a list of ACTION itself
	 * <p>
	 * Then the returned streamSwitch will trigger actions in the order of this action list serially,
	 * If the element is action list, streamSwitch will trigger those actions at the same time (parallel).
	 * <p>
	 * For example: "rescale: (out: 4, in: 2, repartition, (out: 4, in: 3, repartition))"
	 * The name must be started with "rescale"
	 *
	 * @param configInJobVertexName
	 * @return
	 */
	public static ConfigurableDummyStreamSwitch createFromJobVertex(String configInJobVertexName) {
		if (configInJobVertexName.startsWith("rescale")) {
			RescaleActionDescriptor descriptor;
			try {
				descriptor = RescaleActionDescriptor.decode(
					configInJobVertexName.substring(10, configInJobVertexName.length() - 1));
				ConfigurableDummyStreamSwitch dummyStreamSwitch = new ConfigurableDummyStreamSwitch(configInJobVertexName);

				dummyStreamSwitch.setRescaleAction(descriptor.rescaleActionList);
				return dummyStreamSwitch;
			} catch (Exception e) {
				System.out.println("decode error");
				return null;
			}
		} else
			return null;
	}

	@Override
	public void init(OperatorControllerListener listener, List<String> executors, List<String> partitions) {
		this.listener = listener;

		this.executorMapping = new HashMap<>();

		int numExecutors = executors.size();
		int numPartitions = partitions.size();
		for (int executorId = 0; executorId < numExecutors; executorId++) {
			List<String> executorPartitions = new ArrayList<>();
			executorMapping.put(String.valueOf(executorId), executorPartitions);

			KeyGroupRange keyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
				numPartitions, numExecutors, executorId);
			for (int i = keyGroupRange.getStartKeyGroup(); i <= keyGroupRange.getEndKeyGroup(); i++) {
				executorPartitions.add(String.valueOf(i));
			}
		}

		this.random = new Random();
		this.random.setSeed(System.currentTimeMillis());

		this.listener.setup(executorMapping);
	}

	@Override
	public void initMetrics(JobGraph jobGraph, JobVertexID vertexID, Configuration jobConfiguration, int parallelism) {
	}

	@Override
	public void onForceRetrieveMetrics() {
	}

	@Override
	public void stopGracefully() {
		isStopped = true;
	}

	@Override
	public void onMigrationExecutorsStopped() {

	}

	@Override
	public void onMigrationCompleted() {
		waitForMigrationDeployed = false;
	}

	@Override
	public void run() {
		try {
			LOG.info("------ " + name + " start to run");

			// cool down time, wait for fully deployment
			Thread.sleep(5 * 1000);

			for (RescaleActionDescriptor.BaseRescaleAction action : this.actionList) {
				if (action instanceof RescaleActionDescriptor.SimpleRescaleAction) {
					RescaleActionDescriptor.SimpleRescaleAction simpleRescaleAction = (RescaleActionDescriptor.SimpleRescaleAction) action;
					switch (action.actionType) {
						case REPARTITION:
							testRepartition();
							break;
						case SCALE_IN:
							testScaleIn(simpleRescaleAction.newParallelism);
							break;
						case SCALE_OUT:
							testScaleOut(simpleRescaleAction.newParallelism);
							break;
					}
				}
			}
			LOG.info("------ " + name + " finished");
		} catch (Exception e) {
			LOG.info("------ " + name + " exception", e);
		}
	}


	private void triggerAction(String logStr, Runnable runnable, Map<String, List<String>> partitionAssignment) throws InterruptedException {
		LOG.info("------ " + name + "  " + logStr + "   partitionAssignment: " + partitionAssignment);
		waitForMigrationDeployed = true;

		runnable.run();

		while (waitForMigrationDeployed) ;
	}


//	private void preparePartitionAssignment(String... executorIds) {
//		executorMapping.clear();
//
//		for (String executorId : executorIds) {
//			executorMapping.put(executorId, new ArrayList<>());
//		}
//	}

	/**
	 * This method can only be called once.
	 * If new parallelism == old parallelism, it is repartition,
	 *	 will move half target sub task partition to destination sub task
	 * if new parallelism > old parallelism, it is scale out,
	 *   will move half target sub task partition to new added sub task
	 * if new parallelism < old parallelism, it is scale in,
	 *   will move all target sub task partition to destination sub task,
	 *
	 * NOTICE: we suppose that the different between old and new parallelism is not more than one if wanting scale out
	 *
	 * @param newParallelism
	 */
	private void preparePartitionAssignment(int newParallelism) {
		int oldParallelism = executorMapping.size();
		for (int i = 0; i < newParallelism; i++) {
			executorMapping.putIfAbsent(String.valueOf(i), new ArrayList<>());
		}
		final int rangeSize = 128 / oldParallelism;
		if(newParallelism >= oldParallelism){
			// scale out & repartition
			List<String> firstSubTask =  executorMapping.get("0");
			List<String> originKeys = new ArrayList<>(firstSubTask.size());
			List<String> otherKeys = new ArrayList<>(firstSubTask.size());
			for(int i=0;i<firstSubTask.size();i++){
				if(i%2==0){
					originKeys.add(firstSubTask.get(i));
				}else {
					otherKeys.add(firstSubTask.get(i));
				}
			}
			executorMapping.put("0", originKeys);
			executorMapping.get(String.valueOf(newParallelism-1)).addAll(otherKeys);
		}else {
			// scale in
			List<String> originKeys = new ArrayList<>(rangeSize * (oldParallelism-newParallelism));
			for(int i=newParallelism;i<oldParallelism;i++){
				List<String> lastSubTask =  executorMapping.get(String.valueOf(i));
				originKeys.addAll(lastSubTask);
				lastSubTask.clear();
			}
			executorMapping.get("0").addAll(originKeys);
		}
	}

	private void testRepartition() throws InterruptedException {
		/*
		 * init: parallelism 4
		 * 	0: [0, 31]
		 * 	1: [32, 63]
		 *  2: [64, 95]
		 *  3: [96, 127]
		 */

		preparePartitionAssignment(executorMapping.size());
		triggerAction(
			"trigger 1 repartition",
			() -> listener.remap(executorMapping),
			executorMapping);
	}

	private void testScaleOut(int newParallelism) throws InterruptedException {
		preparePartitionAssignment(newParallelism);
		triggerAction(
			"trigger 1 scale out",
			() -> listener.scale(newParallelism, executorMapping),
			executorMapping);
	}

	private void testScaleIn(int newParallelism) throws InterruptedException {
		/*
		 * init: parallelism 4
		 * 	0: [0, 31]
		 * 	1: [32, 63]
		 *  2: [64, 95]
		 *  3: [96, 127]
		 */

		/*
		 * scale in to parallelism 3
		 *   0: [0, 31], [64, 95]
		 *   1: [32, 63]
		 *   3: [96, 127]
		 */
		preparePartitionAssignment(newParallelism);
		triggerAction(
			"trigger 1 scale in",
			() -> listener.scale(3, executorMapping),
			executorMapping);

		Thread.sleep(5000);
		/*
		 * scale out to parallelism 4
		 *   0: [0, 31], [64, 95]
		 *   1: [32, 63]
		 *   3: even of [96, 127]
		 *   4: odd of [96, 127]
		 */
		preparePartitionAssignment(4);
		triggerAction(
			"trigger 2 scale out",
			() -> listener.scale(3, executorMapping),
			executorMapping);
	}

}
