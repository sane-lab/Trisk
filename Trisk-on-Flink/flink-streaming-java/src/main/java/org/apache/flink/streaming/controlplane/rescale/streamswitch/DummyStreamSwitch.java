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

public class DummyStreamSwitch extends Thread implements FlinkOperatorController {

	private static final Logger LOG = LoggerFactory.getLogger(DummyStreamSwitch.class);

	private final String name;

	private OperatorControllerListener listener;

	Map<String, List<String>> executorMapping;

	private volatile boolean waitForMigrationDeployed;

	private volatile boolean isStopped;

	private Random random;

	public DummyStreamSwitch() {
		this("DummyStreamSwitch");
	}

	public DummyStreamSwitch(String name) {
		this.name = name;
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

//			testRepartition();
//			testScaleOut();
			testScaleIn();
//			testCaseOneToOneChange();
//			testJoin();
//			testCaseScaleIn();
//			testRandomScalePartitionAssignment();

			LOG.info("------ " + name + " finished");
		} catch (Exception e) {
			LOG.info("------ " + name + " exception", e);
		}
	}



	private void triggerAction(String logStr, Runnable runnable, Map<String, List<String>> partitionAssignment) throws InterruptedException {
		LOG.info("------ " + name + "  " + logStr + "   partitionAssignment: " + partitionAssignment);
		waitForMigrationDeployed = true;

		runnable.run();

		while (waitForMigrationDeployed);
	}

	private void preparePartitionAssignment(int parallelism) {
		executorMapping.clear();

		for (int i = 0; i < parallelism; i++) {
			executorMapping.put(String.valueOf(i), new ArrayList<>());
		}
	}

	private void preparePartitionAssignment(String ...executorIds) {
		executorMapping.clear();

		for (String executorId: executorIds) {
			executorMapping.put(executorId, new ArrayList<>());
		}
	}

	private void prepareRandomPartitionAssignment(int parallelism) {
		preparePartitionAssignment(parallelism);

		for (int i = 0; i < 128; i++) {
			if (i < parallelism) {
				executorMapping.get(i + "").add(i + "");
			} else {
				executorMapping.get(random.nextInt(parallelism) + "").add(i + "");
			}
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

		preparePartitionAssignment("0", "1", "2", "3");
		for (int i = 0; i < 128; i++) {
			if (i <= 31) {
				if (i % 2 == 0)
					executorMapping.get("0").add(i + "");
				else
					executorMapping.get("2").add(i + "");
			} else if (i <= 63)
				executorMapping.get("1").add(i + "");
			else if (i <= 95)
				executorMapping.get("2").add(i + "");
			else
				executorMapping.get("3").add(i + "");
		}
		triggerAction(
			"trigger 1 repartition",
			() -> listener.remap(executorMapping),
			executorMapping);
	}

	private void testScaleOut() throws InterruptedException {
		/*
		 * init: parallelism 3
		 * 	0: [0, 42]
		 * 	1: [43, 85]
		 *  2: [86, 127]
		 */

		preparePartitionAssignment("0", "1", "2", "3");
		for (int i = 0; i < 128; i++) {
			if (i <= 42) {
				if (i % 2 == 0)
					executorMapping.get("0").add(i + "");
				else
					executorMapping.get("3").add(i + "");
			} else if (i <= 85)
				executorMapping.get("1").add(i + "");
			else
				executorMapping.get("2").add(i + "");
		}
		triggerAction(
			"trigger 1 scale out",
			() -> listener.scale(4, executorMapping),
			executorMapping);
	}

	private void testScaleIn() throws InterruptedException {
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
		preparePartitionAssignment("0", "1", "3");
		for (int i = 0; i < 128; i++) {
			if (i <= 31)
				executorMapping.get("0").add(i + "");
			else if (i <= 63)
				executorMapping.get("1").add(i + "");
			else if (i <= 95)
				executorMapping.get("0").add(i + "");
			else
				executorMapping.get("3").add(i + "");
		}
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
		preparePartitionAssignment("0", "1", "3", "4");
		for (int i = 0; i < 128; i++) {
			if (i <= 31)
				executorMapping.get("0").add(i + "");
			else if (i <= 63)
				executorMapping.get("1").add(i + "");
			else if (i <= 95)
				executorMapping.get("0").add(i + "");
			else
				if (i % 2 == 0)
					executorMapping.get("3").add(i + "");
				else
					executorMapping.get("4").add(i + "");
		}
		triggerAction(
			"trigger 2 scale out",
			() -> listener.scale(4, executorMapping),
			executorMapping);
	}

	private void testCaseOneToOneChange() throws InterruptedException {
		/*
		* init: parallelism 2
		* 	0: [0, 63]
		* 	1: [64, 127]
		*/

		/*
		 * repartition with parallelism 2
		 *   0: [0, 20]
		 *   1: [21, 127]
		 */
		preparePartitionAssignment("0", "1");
		for (int i = 0; i < 128; i++) {
			if (i <= 20)
				executorMapping.get("0").add(i + "");
			else
				executorMapping.get("1").add(i + "");
		}
		triggerAction(
			"trigger 1 repartition",
			() -> listener.remap(executorMapping),
			executorMapping);
//		sleep(10000);

		/*
		 * scale in to parallelism 1
		 *   1: [0, 127]
		 */
		preparePartitionAssignment("1");
		for (int i = 0; i < 128; i++) {
			executorMapping.get("1").add(i + "");
		}
		triggerAction(
			"trigger 2 scale in",
			() -> listener.scale(1, executorMapping),
			executorMapping);
//		sleep(10000);

		/*
		 * scale out to parallelism 2
		 *   1: [0, 50]
		 *   2: [51, 127]
		 */
		preparePartitionAssignment("1", "2");
		for (int i = 0; i < 128; i++) {
			if (i <= 50)
				executorMapping.get("1").add(i + "");
			else
				executorMapping.get("2").add(i + "");
		}
		triggerAction(
			"trigger 3 scale out",
			() -> listener.scale(2, executorMapping),
			executorMapping);
//		sleep(10000);

		/*
		 * scale out to parallelism 3
		 *   1: [0, 50]
		 *   2: [51, 90]
		 *   3: [91, 127]
		 */
		preparePartitionAssignment("1", "2", "3");
		for (int i = 0; i < 128; i++) {
			if (i <= 50)
				executorMapping.get("1").add(i + "");
			else if (i <= 90)
				executorMapping.get("2").add(i + "");
			else
				executorMapping.get("3").add(i + "");
		}
		triggerAction(
			"trigger 4 scale out",
			() -> listener.scale(3, executorMapping),
			executorMapping);
//		sleep(10000);

		/*
		 * scale in to parallelism 2
		 *   1: [0, 90]
		 *   3: [91, 127]
		 */
		preparePartitionAssignment("1", "3");
		for (int i = 0; i < 128; i++) {
			if (i <= 90)
				executorMapping.get("1").add(i + "");
			else
				executorMapping.get("3").add(i + "");
		}
		triggerAction(
			"trigger 5 scale in",
			() -> listener.scale(2, executorMapping),
			executorMapping);
//		sleep(10000);

		/*
		 * scale out to parallelism 3
		 *   1: even in [0, 90]
		 *   3: [91, 127]
		 *   4: odd in [0, 90]
		 */
		preparePartitionAssignment("1", "3", "4");
		for (int i = 0; i < 128; i++) {
			if (i <= 90)
				if (i % 2 == 0)
					executorMapping.get("1").add(i + "");
				else
					executorMapping.get("4").add(i + "");
			else
				executorMapping.get("3").add(i + "");
		}
		triggerAction(
			"trigger 6 scale out",
			() -> listener.scale(3, executorMapping),
			executorMapping);
//		sleep(10000);

		/*
		 * scale out to parallelism 4
		 *   1: even in [0, 90]
		 *   3: even in [91, 127]
		 *   4: odd in [0, 90]
		 *   5: odd in [91, 127]
		 */
		preparePartitionAssignment("1", "3", "4", "5");
		for (int i = 0; i < 128; i++) {
			if (i <= 90)
				if (i % 2 == 0)
					executorMapping.get("1").add(i + "");
				else
					executorMapping.get("4").add(i + "");
			else
				if (i % 2 == 0)
					executorMapping.get("3").add(i + "");
				else
					executorMapping.get("5").add(i + "");
		}
		triggerAction(
			"trigger 7 scale out",
			() -> listener.scale(4, executorMapping),
			executorMapping);
	}

	private void testJoin() throws InterruptedException {
		/*
		 * init: parallelism 1
		 * 	0: [0, 127]
		 */

		preparePartitionAssignment("0", "1");
		for (int i = 0; i < 128; i++) {
			if (i <= 63)
				executorMapping.get("0").add(i + "");
			else
				executorMapping.get("1").add(i + "");
		}
		triggerAction(
			"trigger 1 scale out",
			() -> listener.scale(2, executorMapping),
			executorMapping);
		sleep(10000);

		preparePartitionAssignment("0", "1", "2");
		for (int i = 0; i < 128; i++) {
			if (i <= 63)
				executorMapping.get("0").add(i + "");
			else if (i <= 80)
				executorMapping.get("1").add(i + "");
			else
				executorMapping.get("2").add(i + "");
		}
		triggerAction(
			"trigger 2 scale out",
			() -> listener.scale(3, executorMapping),
			executorMapping);
		sleep(10000);

		preparePartitionAssignment("0", "2");
		for (int i = 0; i < 128; i++) {
			if (i <= 63)
				executorMapping.get("0").add(i + "");
			else
				executorMapping.get("2").add(i + "");
		}
		triggerAction(
			"trigger 3 scale in",
			() -> listener.scale(2, executorMapping),
			executorMapping);
		sleep(10000);

		preparePartitionAssignment("0", "2", "3");
		for (int i = 0; i < 128; i++) {
			if (i <= 63)
				executorMapping.get("0").add(i + "");
			else if (i <= 90)
				executorMapping.get("2").add(i + "");
			else
				executorMapping.get("3").add(i + "");
		}
		triggerAction(
			"trigger 4 scale out",
			() -> listener.scale(3, executorMapping),
			executorMapping);
	}
}
