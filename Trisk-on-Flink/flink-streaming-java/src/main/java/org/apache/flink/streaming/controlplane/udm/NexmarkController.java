package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.TaskDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.resource.AbstractSlot;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ExecutionPlanWithLock;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NexmarkController extends AbstractController {
	private static final Logger LOG = LoggerFactory.getLogger(NexmarkController.class);

	private final Object lock = new Object();
	private final Profiler profiler;
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

	public NexmarkController(ReconfigurationExecutor reconfigurationExecutor, Configuration configuration) {
		super(reconfigurationExecutor);
		profiler = new Profiler();
		experimentConfig = configuration.toMap();
	}

	@Override
	public synchronized void startControllers() {
		System.out.println("PerformanceMeasure is starting...");
		profiler.setName("reconfiguration performance measure");
		profiler.start();
	}

	@Override
	public void stopControllers() {
		System.out.println("PerformanceMeasure is stopping...");
		finished = true;
		profiler.interrupt();
	}

	@Override
	public synchronized void onChangeCompleted(Throwable throwable) {
		if (throwable != null) {
			profiler.interrupt();
			return;
		}
		synchronized (lock) {
			lock.notify();
		}
	}

	protected void generateTest() throws Exception {
		String testOperatorName = experimentConfig.getOrDefault(TEST_OPERATOR_NAME, "filter");
		//		int reconfigFreq = Integer.parseInt(experimentConfig.getOrDefault(RECONFIG_FREQUENCY, "5"));
		int testOpID = findOperatorByName(testOperatorName);
		latestUnusedSubTaskIdx = getReconfigurationExecutor().getExecutionPlan().getParallelism(testOpID);

		// 10s
		Thread.sleep(5000);
//		smartPlacement(testOpID);
		smartPlacementV2(testOpID);
	}

	private void waitForCompletion() throws InterruptedException {
		// wait for operation completed
		synchronized (lock) {
			lock.wait();
		}
	}

	private void smartPlacement(int testOpID) throws Exception {
		ExecutionPlanWithLock planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();

		Map<Integer, Tuple2<Integer, String>> deployment = new HashMap<>();

		Map<String, List<AbstractSlot>> resourceMap = planWithLock.getResourceDistribution();

		OperatorDescriptor operatorDescriptor = planWithLock.getOperatorByID(testOpID);

		int p = planWithLock.getParallelism(testOpID);
		Map<String, AbstractSlot> allocatedSlots = allocateResourceUniformly(resourceMap, p);
		Preconditions.checkNotNull(allocatedSlots, "no more slots can be allocated");
		// place half of tasks with new slots
		List<Integer> modifiedTasks = new ArrayList<>();
		for (int taskId = 0; taskId < p; taskId++) {
			TaskDescriptor task = operatorDescriptor.getTask(taskId);
			// if the task slot is in the allocated slot, this task is unmodified
			if (allocatedSlots.containsKey(task.resourceSlot)) {
				deployment.put(taskId, Tuple2.of(taskId, task.resourceSlot));
				allocatedSlots.remove(task.resourceSlot);
			} else {
				modifiedTasks.add(taskId);
			}
		}

		Preconditions.checkState(modifiedTasks.size() == allocatedSlots.size(),
			"inconsistent task to new slots allocation");

		List<AbstractSlot> allocatedSlotsList = new ArrayList<>(allocatedSlots.values());

		for (int i=0; i<modifiedTasks.size(); i++) {
			int taskId = modifiedTasks.get(i);
			int newTaskId = taskId + p;
			deployment.put(taskId, Tuple2.of(newTaskId, allocatedSlotsList.get(i).getId()));
		}

		placement(testOpID, deployment);
	}

	private void smartPlacementV2(int testOpID) throws Exception {
		ExecutionPlanWithLock planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();

		Map<Integer, String> deployment = new HashMap<>();

		Map<String, List<AbstractSlot>> resourceMap = planWithLock.getResourceDistribution();

		OperatorDescriptor operatorDescriptor = planWithLock.getOperatorByID(testOpID);

		int p = planWithLock.getParallelism(testOpID);
		Map<String, AbstractSlot> allocatedSlots = allocateResourceUniformly(resourceMap, p);
		Preconditions.checkNotNull(allocatedSlots, "no more slots can be allocated");
		// place half of tasks with new slots
		List<Integer> modifiedTasks = new ArrayList<>();
		for (int taskId = 0; taskId < p; taskId++) {
			TaskDescriptor task = operatorDescriptor.getTask(taskId);
			// if the task slot is in the allocated slot, this task is unmodified
			if (allocatedSlots.containsKey(task.resourceSlot)) {
				deployment.put(taskId, task.resourceSlot);
				allocatedSlots.remove(task.resourceSlot);
			} else {
				modifiedTasks.add(taskId);
			}
		}

		Preconditions.checkState(modifiedTasks.size() == allocatedSlots.size(),
			"inconsistent task to new slots allocation");

		List<AbstractSlot> allocatedSlotsList = new ArrayList<>(allocatedSlots.values());

		for (int i=0; i<modifiedTasks.size(); i++) {
			int taskId = modifiedTasks.get(i);
			int newTaskId = taskId + p;
			deployment.put(taskId, allocatedSlotsList.get(i).getId());
		}

//		placement(testOpID, deployment);
		placementV2(testOpID, deployment);
	}


	private Map<String, AbstractSlot> allocateResourceUniformly(Map<String, List<AbstractSlot>> resourceMap, int numTasks) throws Exception {
		// slotId to slot mapping
		Map<String, AbstractSlot> res = new HashMap<>(numTasks);
		int numNodes = resourceMap.size();
		// todo, please ensure numTask could be divided by numNodes for experiment
		if (numTasks % numNodes != 0) {
			throw new Exception("please ensure numTask could be divided by numNodes for experiment");
		}
		int numTasksInOneNode = numTasks / numNodes;
		System.out.println("++++++ number of tasks on each nodes: " + numTasksInOneNode);

		HashMap<String, Integer> loadMap = new HashMap<>();
		HashMap<String, Integer> pendingStots = new HashMap<>();
		HashMap<String, Integer> releasingStots = new HashMap<>();
		// compute the num of tasks in each node
		for (String nodeID : resourceMap.keySet()) {
			List<AbstractSlot> slotList = resourceMap.get(nodeID);
			for (AbstractSlot slot : slotList) {
				if (slot.getState() == AbstractSlot.State.ALLOCATED) {
					loadMap.put(nodeID, loadMap.getOrDefault(nodeID, 0) + 1);
					if (loadMap.getOrDefault(nodeID, 0) <= numTasksInOneNode) {
						res.put(slot.getId(), slot);
					}
				}
			}
		}

		// try to migrate uniformly
		for (String nodeID : resourceMap.keySet()) {
			// the node is overloaded, free future slots, and allocate a new slot in other nodes
			if (loadMap.getOrDefault(nodeID, 0) > numTasksInOneNode) {
				int nReleasingSlots = loadMap.getOrDefault(nodeID, 0) - numTasksInOneNode;
				releasingStots.put(nodeID, releasingStots.getOrDefault(nodeID, 0) + nReleasingSlots);
				for (int i=0; i < nReleasingSlots; i++) {
					findUnusedSlot(numTasksInOneNode, loadMap, pendingStots, nodeID, resourceMap);
				}
			}
		}

		System.out.println("++++++ load map: " + loadMap);

		// free slots from heavy nodes, and allocate slots in light nodes
		for (String nodeID : resourceMap.keySet()) {
			allocateSlotsOnOneNode(resourceMap, res, pendingStots, nodeID);
		}
		if (res.size() == numTasks) {
			// remove them from source map
			// TODO: slot should be marked as ALLOCATED and unused slots should be marked as FREE.
//			for (AbstractSlot slot : res.values()) {
//				resourceMap.get(slot.getLocation()).remove(slot);
//			}
			return res;
		} else {
			return null;
		}
	}

	private void allocateSlotsOnOneNode(Map<String, List<AbstractSlot>> resourceMap, Map<String, AbstractSlot> res, HashMap<String, Integer> pendingStots, String nodeID) {
		List<AbstractSlot> slotList = resourceMap.get(nodeID);
		int allocated = 0;
		for (AbstractSlot slot : slotList) {
			if (allocated >= pendingStots.getOrDefault(nodeID, 0)) {
				continue;
			}
			if (slot.getState() == AbstractSlot.State.FREE) {
				System.out.println("++++++ choosing slot: " + slot);
				res.put(slot.getId(), slot);
				allocated++;
			}
		}
	}

	private void findUnusedSlot(int numTasksInOneNode, HashMap<String, Integer> loadMap,
								HashMap<String, Integer> pendingStots, String nodeID,
								Map<String, List<AbstractSlot>> resourceMap) {
		for (String otherNodeID : resourceMap.keySet()) {
			if (loadMap.getOrDefault(otherNodeID, 0) < numTasksInOneNode) {
				System.out.println("++++++ exceeded number of tasks on node: " + nodeID
					+ " allocate exceeded one to another node: " + otherNodeID);
				pendingStots.put(otherNodeID, pendingStots.getOrDefault(otherNodeID, 0)+1);
				loadMap.put(otherNodeID, loadMap.getOrDefault(otherNodeID, 0) + 1);
				break;
			}
		}
	}

	private class Profiler extends Thread {

		@Override
		public void run() {
			// the testing jobGraph (workload) is in TestingWorkload.java, see that file to know how to use it.
			try {
				Thread.sleep(5000);
				generateTest();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
