package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.TriskWithLock;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

public class DummyController extends AbstractController {

	private final Object object = new Object();
	private final TestingThread testingThread;

	public DummyController(ReconfigurationExecutor reconfigurationExecutor) {
		super(reconfigurationExecutor);
		testingThread = new TestingThread();
	}

	@Override
	public synchronized void startControllers() {
		System.out.println("Testing TestingController is starting...");
		testingThread.setName("reconfiguration test");
		testingThread.start();
	}

	@Override
	public void stopControllers() {
		System.out.println("Testing TestingController is stopping...");
		showOperatorInfo();
	}

	@Override
	public void onChangeStarted() throws InterruptedException {
		// wait for operation completed
		synchronized (object) {
			object.wait();
		}
	}

	@Override
	public synchronized void onChangeCompleted(Throwable throwable) {
		if(throwable != null){
			testingThread.interrupt();
		}
		System.out.println("my self defined instruction finished??");
		synchronized (object) {
			object.notify();
		}
	}

	private void showOperatorInfo() {
		ExecutionPlan streamJobState = getReconfigurationExecutor().getTrisk();
		for (Iterator<OperatorDescriptor> it = streamJobState.getAllOperator(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			System.out.println(descriptor);
			System.out.println("key mapping:" + streamJobState.getKeyMapping(descriptor.getOperatorID()));
			System.out.println("key state allocation" + streamJobState.getKeyStateAllocation(descriptor.getOperatorID()));
			System.out.println("-------------------");
		}
	}

	private void testRebalanceStateful(int testingOpID) throws InterruptedException {
		ExecutionPlan streamJobState = getReconfigurationExecutor().getTrisk();
		Map<Integer, List<Integer>> curKeyStateAllocation = streamJobState.getKeyStateAllocation(testingOpID);
		// we assume that each operator only have one input now

		Map<Integer, List<Integer>> newKeyStateAllocation = new HashMap<>();
		for (Integer taskId : curKeyStateAllocation.keySet()) {
			newKeyStateAllocation.put(taskId, new ArrayList<>(curKeyStateAllocation.get(taskId)));
		}

		List<Integer> oddKeys = newKeyStateAllocation.get(0).stream().filter(i -> i % 2 == 0).collect(Collectors.toList());
		newKeyStateAllocation.get(0).removeAll(oddKeys);
		newKeyStateAllocation.get(1).addAll(oddKeys);
		getReconfigurationExecutor().rebalance(testingOpID, newKeyStateAllocation, true, this);
		// wait for operation completed
		synchronized (object) {
			object.wait();
		}
	}

	private void testRebalanceStateful2(int testingOpID) throws InterruptedException {
		ExecutionPlan streamJobState = getReconfigurationExecutor().getTrisk();
		Map<Integer, List<Integer>> curKeyStateAllocation = streamJobState.getKeyStateAllocation(testingOpID);
		// we assume that each operator only have one input now

		Map<Integer, List<Integer>> newKeyStateAllocation = new HashMap<>();
		for (Integer taskId : curKeyStateAllocation.keySet()) {
			newKeyStateAllocation.put(taskId, new ArrayList<>(curKeyStateAllocation.get(taskId)));
		}

		List<Integer> oddKeys = newKeyStateAllocation.get(1).stream().filter(i -> i % 2 == 0).collect(Collectors.toList());
		newKeyStateAllocation.get(1).removeAll(oddKeys);
		newKeyStateAllocation.get(0).addAll(oddKeys);
		getReconfigurationExecutor().rebalance(testingOpID, newKeyStateAllocation, true, this);
		// wait for operation completed
		synchronized (object) {
			object.wait();
		}
	}

	private void testScaleOutStateful(int testingOpID) throws InterruptedException {
		ExecutionPlan streamJobState = getReconfigurationExecutor().getTrisk();

		int oldParallelism = streamJobState.getParallelism(testingOpID);
		System.out.println(oldParallelism);

		Map<Integer, List<Integer>> curKeyStateAllocation = streamJobState.getKeyStateAllocation(testingOpID);

		assert oldParallelism == curKeyStateAllocation.size() : "old parallelism does not match the key set";

		Map<Integer, List<Integer>> newKeyStateAllocation = new HashMap<>();
		for (Integer taskId : curKeyStateAllocation.keySet()) {
			newKeyStateAllocation.put(taskId, new ArrayList<>(curKeyStateAllocation.get(taskId)));
		}

		List<Integer> oddKeys = newKeyStateAllocation.get(oldParallelism-1).stream()
			.filter(i -> i % 2 == 0) // hardcoded
			.collect(Collectors.toList());
		newKeyStateAllocation.get(oldParallelism-1).removeAll(oddKeys);
		newKeyStateAllocation.put(oldParallelism, oddKeys);

		System.out.println(newKeyStateAllocation);

		getReconfigurationExecutor().rescale(testingOpID, oldParallelism+1, newKeyStateAllocation, this);

		synchronized (object) {
			object.wait();
		}
	}

	private void testScaleOutStateful2(int testingOpID) throws InterruptedException {
		ExecutionPlan streamJobState = getReconfigurationExecutor().getTrisk();

		int oldParallelism = streamJobState.getParallelism(testingOpID);
		System.out.println(oldParallelism);

		Map<Integer, List<Integer>> curKeyStateAllocation = streamJobState.getKeyStateAllocation(testingOpID);

		assert oldParallelism == curKeyStateAllocation.size() : "old parallelism does not match the key set";

		OptionalInt maxKey = curKeyStateAllocation.get(oldParallelism-1).stream().mapToInt(value -> value).max();
		OptionalInt minKey = curKeyStateAllocation.get(oldParallelism-1).stream().mapToInt(value -> value).min();
		int mid = 96;
		if (maxKey.isPresent() && minKey.isPresent()) {
			mid = (maxKey.getAsInt() + minKey.getAsInt())/2;
		}
		Map<Integer, List<Integer>> newKeyStateAllocation = new HashMap<>();
		for (Integer taskId : curKeyStateAllocation.keySet()) {
			newKeyStateAllocation.put(taskId, new ArrayList<>(curKeyStateAllocation.get(taskId)));
		}

		List<Integer> smallKeys = newKeyStateAllocation.get(oldParallelism-1).stream()
			.filter(i -> i <= 64) // hardcoded
			.collect(Collectors.toList());
		newKeyStateAllocation.get(oldParallelism-1).removeAll(smallKeys);
		newKeyStateAllocation.put(oldParallelism, smallKeys);

		System.out.println(newKeyStateAllocation);

		getReconfigurationExecutor().rescale(testingOpID, oldParallelism+1, newKeyStateAllocation, this);

		synchronized (object) {
			object.wait();
		}
	}

	private void testScaleInStateful(int testingOpID) throws InterruptedException {
		ExecutionPlan streamJobState = getReconfigurationExecutor().getTrisk();

		int oldParallelism = streamJobState.getParallelism(testingOpID);
		System.out.println(oldParallelism);

		Map<Integer, List<Integer>> curKeyStateAllocation = streamJobState.getKeyStateAllocation(testingOpID);

		assert oldParallelism == curKeyStateAllocation.size() : "old parallelism does not match the key set";

		Map<Integer, List<Integer>> newKeyStateAllocation = new HashMap<>();
		for (Integer taskId : curKeyStateAllocation.keySet()) {
			newKeyStateAllocation.put(taskId, new ArrayList<>(curKeyStateAllocation.get(taskId)));
		}

		List<Integer> removedKeys = newKeyStateAllocation.remove(oldParallelism-1);
		newKeyStateAllocation.get(oldParallelism-2).addAll(removedKeys);

		System.out.println(newKeyStateAllocation);

		getReconfigurationExecutor().rescale(testingOpID, oldParallelism-1, newKeyStateAllocation, this);

		synchronized (object) {
			object.wait();
		}
	}

	private void testScaleInStateful2(int testingOpID) throws InterruptedException {
		ExecutionPlan streamJobState = getReconfigurationExecutor().getTrisk();

		int oldParallelism = streamJobState.getParallelism(testingOpID);
		System.out.println(oldParallelism);

		Map<Integer, List<Integer>> curKeyStateAllocation = streamJobState.getKeyStateAllocation(testingOpID);

		assert oldParallelism == curKeyStateAllocation.size() : "old parallelism does not match the key set";

		Map<Integer, List<Integer>> newKeyStateAllocation = new HashMap<>();
		for (Integer taskId : curKeyStateAllocation.keySet()) {
			newKeyStateAllocation.put(taskId, new ArrayList<>(curKeyStateAllocation.get(taskId)));
		}

		List<Integer> removedKeys = newKeyStateAllocation.remove(oldParallelism-2);
		newKeyStateAllocation.get(oldParallelism-1).addAll(removedKeys);
		removedKeys = newKeyStateAllocation.remove(oldParallelism-1);
		newKeyStateAllocation.get(0).addAll(removedKeys);

		System.out.println(newKeyStateAllocation);

		getReconfigurationExecutor().rescale(testingOpID, oldParallelism-2, newKeyStateAllocation, this);

		synchronized (object) {
			object.wait();
		}
	}

	private void testScaling(int testingOpID, int newParallelism) throws InterruptedException {
		ExecutionPlan streamJobState = getReconfigurationExecutor().getTrisk();

		Map<Integer, List<Integer>> curKeyStateAllocation = streamJobState.getKeyStateAllocation(testingOpID);
		int oldParallelism = streamJobState.getParallelism(testingOpID);
		assert oldParallelism == curKeyStateAllocation.size() : "old parallelism does not match the key set";

		Map<Integer, List<Integer>> newKeyStateAllocation = preparePartitionAssignment(newParallelism);

		int maxParallelism = 128;

		for (int i = 0; i < maxParallelism; i++) {
			newKeyStateAllocation.get(i%newParallelism).add(i);
		}

		System.out.println(newKeyStateAllocation);

		if (oldParallelism == newParallelism) {
			getReconfigurationExecutor().rebalance(testingOpID, newKeyStateAllocation, true, this);
		} else {
			getReconfigurationExecutor().rescale(testingOpID, newParallelism, newKeyStateAllocation, this);
		}

		synchronized (object) {
			object.wait();
		}
	}

	private void placement(int testingOpID) throws InterruptedException {
		ExecutionPlan streamJobState = getReconfigurationExecutor().getTrisk();

		Map<Integer, List<Integer>> curKeyStateAllocation = streamJobState.getKeyStateAllocation(testingOpID);
		int oldParallelism = streamJobState.getParallelism(testingOpID);
		assert oldParallelism == curKeyStateAllocation.size() : "old parallelism does not match the key set";

		Map<Integer, List<Integer>> newKeyStateAllocation = new HashMap<>();

		newKeyStateAllocation.put(0, curKeyStateAllocation.get(0));
		newKeyStateAllocation.put(2, curKeyStateAllocation.get(1));

		Map<Integer, Tuple2<Integer, String>> deployment = new HashMap<>();
		deployment.put(0, Tuple2.of(0, ""));
		// migrate 1 to 2 with the assigned slot, but now we let slot to be assigned by default.
		deployment.put(1, Tuple2.of(2, ""));


//		getReconfigurationExecutor().placement(testingOpID, newKeyStateAllocation, this);
		placement(testingOpID, deployment);
		synchronized (object) {
			object.wait();
		}
	}

	private void rescaleV2(int operatorId, int newParallelism) throws InterruptedException {
		// get the execution plan, will throw an exception if the execution plan is in used
		TriskWithLock executionPlan = getReconfigurationExecutor().getExecutionPlanCopy();

//		Map<Integer, List<Integer>> curKeyDistribution = executionPlan.getKeyDistribution(operatorId);
		int oldParallelism = executionPlan.getParallelism(operatorId);
//		assert oldParallelism == curKeyDistribution.size() : "old parallelism does not match the key set";

		Map<Integer, List<Integer>> newKeyDistribution = preparePartitionAssignment(newParallelism);

		int maxParallelism = 128;
		for (int i = 0; i < maxParallelism; i++) {
			newKeyDistribution.get(i%newParallelism).add(i);
		}
//		executionPlan
//			.redistribute(operatorId, newKeyDistribution)
//			.redeploy(operatorId, null, newParallelism>oldParallelism);
//
//		getReconfigurationExecutor().execute(this, executionPlan);
//
//		onChangeStarted();
		if (newParallelism != oldParallelism) {
			scaling(operatorId, newKeyDistribution, null);
		} else {
			loadBalancing(operatorId, newKeyDistribution);
		}
	}

	private void measureFunctionUpdate(int testOpID) throws InterruptedException {
		TriskWithLock executionPlan = getReconfigurationExecutor().getExecutionPlanCopy();
		try {
			ClassLoader userClassLoader = executionPlan.getUserFunction(testOpID).getClass().getClassLoader();
			Class IncreaseCommunicationOverheadMapClass = userClassLoader.loadClass("flinkapp.StatefulDemoLongRun$IncreaseCommunicationOverheadMap");
			Class IncreaseComputationOverheadMap = userClassLoader.loadClass("flinkapp.StatefulDemoLongRun$IncreaseComputationOverheadMap");
			int i = 0;
			Random random = new Random();
			Object func;
			if (random.nextInt(2) > 0) {
				func = IncreaseCommunicationOverheadMapClass.getConstructor(int.class)
					.newInstance(random.nextInt(10) + 1);
			} else {
				func = IncreaseComputationOverheadMap.getConstructor(int.class)
					.newInstance(random.nextInt(10));
			}

			System.out.println("\nnumber of function update test: " + i);
			System.out.println("new function:" + func);
			changeOfLogic(testOpID, func);
		} catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
			e.printStackTrace();
		}
	}

	private Map<Integer, List<Integer>> preparePartitionAssignment(int parallleism) {
		Map<Integer, List<Integer>> newKeyStateAllocation = new HashMap<>();
		for (int i = 0; i < parallleism; i++) {
			newKeyStateAllocation.put(i, new ArrayList<>());
		}
		return newKeyStateAllocation;
	}

	// WARNING: This only works without rebalance of the stateless operator
	private void testScaleOut2(int testingOpID) throws InterruptedException {
		ExecutionPlan streamJobState = getReconfigurationExecutor().getTrisk();

		int oldParallelism = streamJobState.getParallelism(testingOpID);
		System.out.println(oldParallelism);

		Map<Integer, List<Integer>> curKeyStateAllocation = streamJobState.getKeyStateAllocation(testingOpID);

		assert oldParallelism == curKeyStateAllocation.size() : "old parallelism does not match the key set";

		Map<Integer, List<Integer>> newKeyStateAllocation = new HashMap<>();
		for (Integer taskId : curKeyStateAllocation.keySet()) {
			newKeyStateAllocation.put(taskId, new ArrayList<>(curKeyStateAllocation.get(taskId)));
		}

		List<Integer> smallHalf1 = newKeyStateAllocation.get(0).stream()
			.filter(i -> i < 31) // hardcoded
			.collect(Collectors.toList());
		newKeyStateAllocation.get(0).removeAll(smallHalf1);
		newKeyStateAllocation.put(oldParallelism, smallHalf1);
		List<Integer> smallHalf2 = newKeyStateAllocation.get(1).stream()
			.filter(i -> i < 96) // hardcoded
			.collect(Collectors.toList());
		newKeyStateAllocation.get(1).removeAll(smallHalf2);
		newKeyStateAllocation.put(oldParallelism+1, smallHalf2);

		System.out.println(newKeyStateAllocation);

		getReconfigurationExecutor().rescale(testingOpID, oldParallelism+2, newKeyStateAllocation, this);

		synchronized (object) {
			object.wait();
		}
	}

	@Deprecated
	private void testRebalanceStateless(int testingOpID) throws InterruptedException {
		ExecutionPlan streamJobState = getReconfigurationExecutor().getTrisk();
		Set<OperatorDescriptor> parents = streamJobState.getOperatorByID(testingOpID).getParents();
		// we assume that each operator only have one input now
		for (OperatorDescriptor parent : parents) {
			Map<Integer, List<Integer>> curKeyStateAllocation = parent.getKeyMapping().get(testingOpID);
//				.get(testingOpID)
//				.stream()
//				.map(ArrayList::new)
//				.collect(Collectors.toList());
			Map<Integer, List<Integer>> newKeyStateAllocation = new HashMap<>();
			for (Integer taskId : curKeyStateAllocation.keySet()) {
				newKeyStateAllocation.put(taskId, new ArrayList<>(curKeyStateAllocation.get(taskId)));
			}

			List<Integer> oddKeys = newKeyStateAllocation.get(0).stream().filter(i -> i % 2 == 0).collect(Collectors.toList());
			newKeyStateAllocation.get(0).removeAll(oddKeys);
			newKeyStateAllocation.get(1).addAll(oddKeys);
			getReconfigurationExecutor().rebalance(testingOpID, newKeyStateAllocation, false, this);
			// wait for operation completed
			synchronized (object) {
				object.wait();
			}
			break;
		}
	}

	@Deprecated
	private void testPauseSource(int sourceID) throws InterruptedException {
		getReconfigurationExecutor().callCustomizeOperations(
			enforcement -> FutureUtils.completedVoidFuture()
				.thenCompose(o -> enforcement.synchronizeTasks(Collections.singletonList(Tuple2.of(sourceID, -1)), null))
				.thenCompose(o -> {
					try {
						Thread.sleep(3);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					return enforcement.resumeTasks();
				})
				.whenComplete((o, failure) -> {
					if(failure != null){
						failure.printStackTrace();
					}
					synchronized (object) {
						object.notify();
					}
				})
		);
		// wait for operation completed
		synchronized (object) {
			object.wait();
			System.out.println("pause source successful");
		}
	}

	private void testCustomizeWindowUpdateAPI() throws InterruptedException {
		//	 an example shows how to defined customize operations
		int windowOpID = findOperatorByName("counting window reduce");
		if (windowOpID != -1) {
			OperatorDescriptor descriptor = getReconfigurationExecutor().getTrisk().getOperatorByID(windowOpID);
			Map<String, Object> attributeMap = descriptor.getControlAttributeMap();
			PurgingTrigger<?, ?> trigger = (PurgingTrigger<?, ?>) attributeMap.get("trigger");
			long oldWindowSize = ((CountTrigger<?>) trigger.getNestedTrigger()).getMaxCount();
			System.out.println("update window size from " + oldWindowSize + " to " + (oldWindowSize / 2));
			try {
				updateCountingWindowSize(windowOpID, oldWindowSize / 2);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// self customize high level reconfiguration api
	private void updateCountingWindowSize(int rawVertexID, long newWindowSize) throws Exception {
		// update abstraction in stream manager execution plan
		CountTrigger<?> trigger = CountTrigger.of(newWindowSize);
		OperatorDescriptor descriptor = getReconfigurationExecutor().getTrisk().getOperatorByID(rawVertexID);
		descriptor.setControlAttribute("trigger", PurgingTrigger.of(trigger));
		getReconfigurationExecutor().callCustomizeOperations(
			enforcement -> FutureUtils.completedVoidFuture()
				.thenCompose(o -> enforcement.prepareExecutionPlan(getReconfigurationExecutor().getTrisk()))
				.thenCompose(o -> enforcement.synchronizeTasks(Collections.singletonList(Tuple2.of(rawVertexID, -1)), o))
				.thenCompose(o -> enforcement.updateFunction(rawVertexID, o))
				.whenComplete((o, failure) -> {
					if(failure != null){
						failure.printStackTrace();
					}
					synchronized (object) {
						this.onChangeCompleted(failure);
					}
				})
		);
		// wait for operation completed
		synchronized (object) {
			object.wait();
		}
	}

	private void testScaleOutWindowJoin() throws InterruptedException {

		ExecutionPlan streamJobState = getReconfigurationExecutor().getTrisk();
		int testingOpID = findOperatorByName("join1");

		int oldParallelism = streamJobState.getParallelism(testingOpID);
		System.out.println(oldParallelism);

		Map<Integer, List<Integer>> curKeyStateAllocation = streamJobState.getKeyStateAllocation(testingOpID);

		assert oldParallelism == curKeyStateAllocation.size() : "old parallelism does not match the key set";

		Map<Integer, List<Integer>> newKeyStateAllocation = new HashMap<>();
		for (Integer taskId : curKeyStateAllocation.keySet()) {
			newKeyStateAllocation.put(taskId, new ArrayList<>(curKeyStateAllocation.get(taskId)));
		}

		List<Integer> oddKeys = newKeyStateAllocation.get(0).stream().filter(i -> i % 2 == 0).collect(Collectors.toList());
		newKeyStateAllocation.get(0).removeAll(oddKeys);
		newKeyStateAllocation.put(oldParallelism, oddKeys);

		System.out.println(newKeyStateAllocation);

		getReconfigurationExecutor().rescale(testingOpID, oldParallelism+1, newKeyStateAllocation, this);

		synchronized (object) {
			object.wait();
		}
	}

	private void testNoOp(int operatorID) throws InterruptedException {

		getReconfigurationExecutor().noOp(operatorID, this);

		synchronized (object) {
			object.wait();
		}
	}

	private class TestingThread extends Thread {

		@Override
		public void run() {
			// the testing jobGraph (workload) is in TestingWorkload.java, see that file to know how to use it.
			int statefulOpID = findOperatorByName("Splitter FlatMap");

			try {
				showOperatorInfo();
				// todo, if the time of sleep is too short, may cause receiving not belong key
				Thread.sleep(50000);

//				for (int i=0; i<100; i++) {

//				int i = 0;
//				while(true) {
//					rescaleV2(statefulOpID, (i%10 + 1));
//					sleep(100);
//					i++;
//				}
//				measureFunctionUpdate(statefulOpID);
				rescaleV2(statefulOpID, 10);
//				sleep(100);
//				rescaleV2(statefulOpID, 5);
//				sleep(100);
//				rescaleV2(statefulOpID, 10);

//				placement(statefulOpID);

//				testScaling(statefulOpID, 2);
//				testScaling(statefulOpID, 10);

//				sleep(100);
//				testScaling(statefulOpID, 3);
//				sleep(100);
//				testScaling(statefulOpID, 6);
//				sleep(100);
//				testScaling(statefulOpID, 2);
//				sleep(100);
//
//				testScaling(statefulOpID, 8);
//				sleep(100);
//
//				testScaling(statefulOpID, 4);

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
