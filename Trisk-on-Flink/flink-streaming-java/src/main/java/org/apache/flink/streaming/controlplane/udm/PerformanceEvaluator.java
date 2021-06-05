package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkState;

public class PerformanceEvaluator extends AbstractController {
	private static final Logger LOG = LoggerFactory.getLogger(PerformanceEvaluator.class);

	private final Map<String, String> experimentConfig;

	public final static String AFFECTED_TASK = "trisk.reconfig.affected_tasks";
	public final static String TEST_OPERATOR_NAME = "trisk.reconfig.operator.name";
	public final static String RECONFIG_FREQUENCY = "trisk.reconfig.frequency";
	public final static String RECONFIG_INTERVAL = "trisk.reconfig.interval";
	public final static String TEST_TYPE = "trisk.reconfig.type";

	private final static String REMAP = "remap";
	private final static String RESCALE = "rescale";
	private final static String NOOP = "noop";
	private final static String EXECUTION_LOGIC = "logic";

	private boolean finished = false;

	private int latestUnusedSubTaskIdx = 0;

	public PerformanceEvaluator(ReconfigurationExecutor reconfigurationExecutor, Configuration configuration) {
		super(reconfigurationExecutor);
		experimentConfig = configuration.toMap();
	}

	protected void generateTest() throws InterruptedException {
		String testOperatorName = experimentConfig.getOrDefault(TEST_OPERATOR_NAME, "filter");
		int numAffectedTasks = Integer.parseInt(experimentConfig.getOrDefault(AFFECTED_TASK, "3"));
		int reconfigInterval = Integer.parseInt(experimentConfig.getOrDefault(RECONFIG_INTERVAL, "10000"));
//		int reconfigFreq = Integer.parseInt(experimentConfig.getOrDefault(RECONFIG_FREQUENCY, "5"));
		int testOpID = findOperatorByName(testOperatorName);
		latestUnusedSubTaskIdx = getReconfigurationExecutor().getTrisk().getParallelism(testOpID);
		switch (experimentConfig.getOrDefault(TEST_TYPE, RESCALE)) {
			case REMAP:
				measureRebalance(testOpID, numAffectedTasks, reconfigInterval);
				break;
			case RESCALE:
//				measureRescale(testOpID, numAffectedTasks, 10, reconfigInterval);
				measureRescale(testOpID, numAffectedTasks, reconfigInterval);
				break;
			case EXECUTION_LOGIC:
				measureFunctionUpdate(testOpID, reconfigInterval);
				break;
			case NOOP:
				measureNoOP(testOpID, reconfigInterval);
				break;
		}
	}

	private void measureRebalance(int testOpID, int numAffectedTasks, int reconfigInterval) throws InterruptedException {
		ExecutionPlan executionPlan = getReconfigurationExecutor().getTrisk();
		if (reconfigInterval > 0) {
			int i = 0;
			long start;
			while (true) {
				start = System.currentTimeMillis();
				Map<Integer, List<Integer>> keyStateDistribution = executionPlan.getKeyStateDistribution(testOpID);
				Map<Integer, List<Integer>> newKeySet = new HashMap<>();
				for (Integer taskId : keyStateDistribution.keySet()) {
					newKeySet.put(taskId, new ArrayList<>(keyStateDistribution.get(taskId)));
				}
				// random select numAffectedTask sub key set, and then shuffle them to the same number of key set
				shuffleKeySet(newKeySet, numAffectedTasks);
				System.out.println("\nnumber of rebalance test: " + i);
				System.out.println("new key set:" + newKeySet);

//				getReconfigurationExecutor().rebalance(testOpID, newKeySet, true, this);
//				waitForCompletion();
				loadBalancing(testOpID, newKeySet);
				while ((System.currentTimeMillis() - start) < reconfigInterval) {}
				i++;
			}
		}
	}


	private void measureFunctionUpdate(int testOpID, int reconfigInterval) throws InterruptedException {
		ExecutionPlan executionPlan = getReconfigurationExecutor().getTrisk();
		try {
			ClassLoader userClassLoader = executionPlan.getUserFunction(testOpID).getClass().getClassLoader();
			Class IncreaseCommunicationOverheadMapClass = userClassLoader.loadClass("flinkapp.StatefulDemoLongRun$IncreaseCommunicationOverheadMap");
			Class IncreaseComputationOverheadMap = userClassLoader.loadClass("flinkapp.StatefulDemoLongRun$IncreaseComputationOverheadMap");
			if (reconfigInterval > 0) {
//				int timeInterval = 1000 / reconfigInterval;
				int timeInterval = reconfigInterval;
				int i = 0;
				Random random = new Random();
				while (true) {
					long start = System.currentTimeMillis();
					Object func = null;
					if (random.nextInt(2) > 0) {
						func = IncreaseCommunicationOverheadMapClass.getConstructor(int.class)
							.newInstance(random.nextInt(10) + 1);
					} else {
						func = IncreaseComputationOverheadMap.getConstructor(int.class)
							.newInstance(random.nextInt(10));
					}
					System.out.println("\nnumber of function update test: " + i);
					System.out.println("new function:" + func);
//					getReconfigurationExecutor().reconfigureUserFunction(testOpID, func, this);
//					// wait for operation completed
//					waitForCompletion();
					changeOfLogic(testOpID, func);
					while ((System.currentTimeMillis() - start) < timeInterval) {
					}
					i++;
				}
			}
		} catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
			e.printStackTrace();
		}
	}

	private void measureRescale(int testOpID, int numAffectedTasks, int reconfigInterval) throws InterruptedException {
		ExecutionPlan executionPlan = getReconfigurationExecutor().getTrisk();
		if (reconfigInterval > 0) {
			long start;
			// first scale in and then scale out to avoid exceed the parallelism set for the app,
			// this is only for current app
			boolean isScaleIn = false;
			while (true) {
				start = System.currentTimeMillis();
//				Map<Integer, List<Integer>> keySet = executionPlan.getKeyStateAllocation(testOpID);
				int curp = executionPlan.getParallelism(testOpID);
				int newp = isScaleIn ? curp-numAffectedTasks : curp+numAffectedTasks;
				scaling(testOpID, newp);
				isScaleIn = !isScaleIn;
				while ((System.currentTimeMillis() - start) < reconfigInterval) {
					// busy waiting
				}
			}
		}
	}

	private void scaling(int testingOpID, int newParallelism) throws InterruptedException {
		ExecutionPlan executionPlan = getReconfigurationExecutor().getTrisk();

		Map<Integer, List<Integer>> keyStateDistribution = executionPlan.getKeyStateDistribution(testingOpID);
		int oldParallelism = executionPlan.getParallelism(testingOpID);
		assert oldParallelism == keyStateDistribution.size() : "old parallelism does not match the key set";

		Map<Integer, List<Integer>> newKeyStateDistribution = preparePartitionAssignment(newParallelism);

		int maxParallelism = 128;

		for (int i = 0; i < maxParallelism; i++) {
			newKeyStateDistribution.get(i%newParallelism).add(i);
		}

		// update the parallelism
		executionPlan.setParallelism(testingOpID, newParallelism);

		System.out.println(newKeyStateDistribution);

		if (oldParallelism == newParallelism) {
//			getReconfigurationExecutor().rebalance(executionPlan, testingOpID, this);
			loadBalancing(testingOpID, newKeyStateDistribution);
		} else {
//			getReconfigurationExecutor().rescale(executionPlan, testingOpID, isScaleIn, this);
			scaling(testingOpID, newKeyStateDistribution, null);
		}
	}

	private Map<Integer, List<Integer>> preparePartitionAssignment(int parallleism) {
		Map<Integer, List<Integer>> newKeyStateAllocation = new HashMap<>();
		for (int i = 0; i < parallleism; i++) {
			newKeyStateAllocation.put(i, new ArrayList<>());
		}
		return newKeyStateAllocation;
	}

	private void measureRescale(int testOpID, int numAffectedTasks, int maxParallelism, int reconfigInterval) throws InterruptedException {
		ExecutionPlan executionPlan = getReconfigurationExecutor().getTrisk();
		if (reconfigInterval > 0) {
//			int timeInterval = 1000 / reconfigInterval;
			int i = 0;
			long start;
			while (true) {
//			for (int i = 0; i < reconfigFreq; i++) {
				start = System.currentTimeMillis();
				Map<Integer, List<Integer>> keySet = executionPlan.getKeyStateAllocation(testOpID);
				Map<Integer, List<Integer>> newKeySet = new HashMap<>();
				for (Integer taskId : keySet.keySet()) {
					newKeySet.put(taskId, new ArrayList<>(keySet.get(taskId)));
				}
				Random random = new Random();
				int current = executionPlan.getParallelism(testOpID);
				int newParallelism = 1 + random.nextInt(maxParallelism);
				boolean isScaleOut = true;
				if (newParallelism > current) {
					shuffleKeySetWhenScaleOut(newKeySet, newParallelism, numAffectedTasks);
				} else if (newParallelism < current) {
//					continue;
					shuffleKeySetWhenScaleIn(newKeySet, newParallelism, numAffectedTasks);
					isScaleOut = false;
				} else {
					continue;
				}
				// random select numAffectedTask sub key set, and then shuffle them to the same number of key set
				System.out.println("\nnumber of rescale test: " + i);
				System.out.println("new key set:" + newKeySet);

				LOG.info("++++++number of rescale test: " + i + " type: " + (isScaleOut? "scale_out" : "scale_in"));
				LOG.info("++++++new key set: " + newKeySet);

//				getReconfigurationExecutor().rescale(testOpID, newParallelism, newKeySet, this);
//				// wait for operation completed
//				waitForCompletion();
				scaling(testOpID, newKeySet, null);
				while ((System.currentTimeMillis() - start) < reconfigInterval) {
					// busy waiting
				}
				i++;
			}
		}
	}

	private void measureNoOP(int testOpID, int reconfigInterval) throws InterruptedException {
		ExecutionPlan executionPlan = getReconfigurationExecutor().getTrisk();
		if (reconfigInterval > 0) {
//			int timeInterval = 1000 / reconfigInterval;
			int i = 0;
			long start;
			while (true) {
//			for (int i = 0; i < reconfigFreq; i++) {
				start = System.currentTimeMillis();
				System.out.println("\nnumber of noop test: " + i);
//				getReconfigurationExecutor().noOp(testOpID, this);
				// wait for operation completed
//				waitForCompletion();
				if (System.currentTimeMillis() - start > reconfigInterval) {
					System.out.println("overloaded frequency");
				}
				while ((System.currentTimeMillis() - start) < reconfigInterval) {
				}
				i++;
			}
		}
	}

	private void shuffleKeySet(Map<Integer, List<Integer>> newKeySet, int numAffectedTasks) {
		Random random = new Random();
		numAffectedTasks = Math.min(numAffectedTasks, newKeySet.size());
		List<Integer> selectTaskID = new ArrayList<>(numAffectedTasks);
		Set<Integer> allKeyGroup = new HashSet<>();
		List<Integer> allTaskID = new ArrayList<>(newKeySet.keySet());
		for (int i = 0; i < numAffectedTasks; i++) {
			int offset = random.nextInt(newKeySet.size());
			selectTaskID.add(allTaskID.get(offset));
			allKeyGroup.addAll(newKeySet.remove(allTaskID.remove(offset)));
		}
		List<Integer> keyGroupList = new ArrayList<>(allKeyGroup);
		Collections.shuffle(keyGroupList);
//		int leftBound = 0;
//		int subKeySetSize = keyGroupList.size()/numAffectedTasks; // uniformly assign keygroups to affected tasks.
		int keyGroupSize = keyGroupList.size(); // uniformly assign keygroups to affected tasks.
		for (int i = 0; i < numAffectedTasks; i++) {
			// sub keyset size is in [1, left - num_of_to_keyset_that_are_to_be_decided]
			// we should make sure each sub key set has at at least one element (key group)
//			int subKeySetSize = random.nextInt(keyGroupList.size() - leftBound - numAffectedTasks + i + 1) + 1;
//			subKeySetSize = subKeySetSize > 0 ? subKeySetSize : 1;
//			int start = i*subKeySetSize;
//			int end = i == numAffectedTasks-1 ? keyGroupList.size() : (i+1)*subKeySetSize;
			int start = ((i * keyGroupSize + numAffectedTasks - 1) / numAffectedTasks);
			int end = ((i + 1) * keyGroupSize - 1) / numAffectedTasks;
			newKeySet.put(
				selectTaskID.get(i),
//				new ArrayList<>(keyGroupList.subList(leftBound, leftBound + subKeySetSize))
				new ArrayList<>(keyGroupList.subList(start, end+1))
			);
//			leftBound += subKeySetSize;
		}
	}

	private List<Integer> findNextSubTaskID(Collection<Integer> keySetID, int numOfNext) {
		List<Integer> next = new LinkedList<>();
		int newParallelism = latestUnusedSubTaskIdx + numOfNext;
		while(latestUnusedSubTaskIdx < newParallelism) {
			checkState(!keySetID.contains(latestUnusedSubTaskIdx) && !next.contains(latestUnusedSubTaskIdx),
				"subtask index has already been used.");
			next.add(latestUnusedSubTaskIdx);
			latestUnusedSubTaskIdx++;
		}
		return next;
	}

	private void shuffleKeySetWhenScaleOut(Map<Integer, List<Integer>> newKeySet, int newParallelism, int numAffectedTasks) {
		Random random = new Random();
		// add new added key set id
		List<Integer> selectTaskID = new ArrayList<>(findNextSubTaskID(newKeySet.keySet(), newParallelism - newKeySet.size()));
		Set<Integer> allKeyGroup = new HashSet<>();
		List<Integer> allTaskID = new ArrayList<>(newKeySet.keySet());
		numAffectedTasks = Math.min(numAffectedTasks, newKeySet.size());
		// add affected task key set id
//		for (int i = 0; i < numAffectedTasks; i++) {
//			int offset = random.nextInt(newKeySet.size());
//			selectTaskID.add(allTaskID.get(offset));
//			allKeyGroup.addAll(newKeySet.remove(allTaskID.remove(offset)));
//		}
		int numOfAddedSubset = 0;
		while (numOfAddedSubset < numAffectedTasks || allKeyGroup.size() < selectTaskID.size()) {
			// allKeyGroup.size() < selectTaskID.size() means we don't have enough key groups
			int offset = random.nextInt(newKeySet.size());
			selectTaskID.add(allTaskID.get(offset));
			allKeyGroup.addAll(newKeySet.remove(allTaskID.remove(offset)));
			numOfAddedSubset++;
		}

		List<Integer> keyGroupList = new LinkedList<>(allKeyGroup);
		int leftBound = 0;
		for (int i = 0; i < selectTaskID.size() - 1; i++) {
			// sub keyset size is in [1, left - num_of_to_keyset_that_are_to_be_decided]
			// we should make sure each sub key set has at at least one element (key group)
			int subKeySetSize = 1 + random.nextInt(keyGroupList.size() - leftBound - selectTaskID.size() + i + 1);
//			subKeySetSize = subKeySetSize > 0 ? subKeySetSize : 1;
			newKeySet.put(
				selectTaskID.get(i),
				new ArrayList<>(keyGroupList.subList(leftBound, leftBound + subKeySetSize))
			);
			leftBound += subKeySetSize;
		}
		newKeySet.put(
			selectTaskID.get(selectTaskID.size() - 1),
			new ArrayList<>(keyGroupList.subList(leftBound, keyGroupList.size()))
		);
	}

	private void shuffleKeySetWhenScaleIn(Map<Integer, List<Integer>> newKeySet, int newParallelism, int numAffectedTasks) {
		Random random = new Random();
		int numOfRemove = newKeySet.size() - newParallelism;
		numAffectedTasks = Math.min(numAffectedTasks, newKeySet.size());
		numAffectedTasks = numAffectedTasks > numOfRemove ? numAffectedTasks : numOfRemove + 1;
		int keeped = numAffectedTasks - numOfRemove;
		List<Integer> selectTaskID = new ArrayList<>(numAffectedTasks);
		Set<Integer> allKeyGroup = new HashSet<>();
		List<Integer> allTaskID = new ArrayList<>(newKeySet.keySet());
		for (int i = 0; i < numAffectedTasks; i++) {
			int offset = random.nextInt(newKeySet.size());
			selectTaskID.add(allTaskID.get(offset));
			allKeyGroup.addAll(newKeySet.remove(allTaskID.remove(offset)));
		}
		List<Integer> keyGroupList = new LinkedList<>(allKeyGroup);
		int leftBound = 0;
		Collections.sort(selectTaskID);
		for (int i = 0; i < keeped - 1; i++) {
			// sub keyset size is in [1, left - num_of_to_keyset_that_are_to_be_decided]
			// we should make sure each sub key set has at at least one element (key group)
			int subKeySetSize = 1 + random.nextInt(keyGroupList.size() - leftBound - keeped + i + 1);
//			subKeySetSize = subKeySetSize > 0 ? subKeySetSize : 1;
			newKeySet.put(
				selectTaskID.get(i),
				new ArrayList<>(keyGroupList.subList(leftBound, leftBound + subKeySetSize))
			);
			leftBound += subKeySetSize;
		}
		newKeySet.put(
			selectTaskID.get(keeped - 1),
			new ArrayList<>(keyGroupList.subList(leftBound, keyGroupList.size()))
		);
	}

	@Override
	public void defineControlAction() {
		// the testing jobGraph (workload) is in TestingWorkload.java, see that file to know how to use it.
		try {
			Thread.sleep(10000);
			generateTest();
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			if (finished) {
				System.err.println("PerformanceEvaluator stopped");
				return;
			}
			e.printStackTrace();
			System.err.println("interrupted, thread exit");
		}
	}

}
