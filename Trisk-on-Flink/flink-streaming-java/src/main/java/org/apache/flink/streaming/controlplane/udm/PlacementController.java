package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.TaskDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.resource.AbstractSlot;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ExecutionPlanWithLock;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class PlacementController extends AbstractController {

	private int requestTime = 0;

	public PlacementController(ReconfigurationExecutor reconfigurationExecutor) {
		super(reconfigurationExecutor);
	}

	@Override
	public void startControllers() {
		super.controlActionRunner.start();
	}

	@Override
	public void stopControllers() {
		super.controlActionRunner.interrupt();
	}

	@Override
	protected void defineControlAction() throws Exception {
		Thread.sleep(10 * 1000);
		requestTime += 2 * 60;
		Thread.sleep(10 * 1000);
		smartPlacementV2(findOperatorByName("dtree"));
		Thread.sleep(10 * 1000);
		smartPlacementV2(findOperatorByName("preprocess"));
	}

	private void smartPlacementV2(int testOpID) throws Exception {
		ExecutionPlanWithLock planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
		Map<Integer, String> deployment = new HashMap<>();
		Map<String, List<AbstractSlot>> resourceMap = planWithLock.getResourceDistribution();
		OperatorDescriptor operatorDescriptor = planWithLock.getOperatorByID(testOpID);
		int p = planWithLock.getParallelism(testOpID);
		Map<String, AbstractSlot> allocatedSlots = allocateResourceUniformlyV2(resourceMap, p);
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
			deployment.put(taskId, allocatedSlotsList.get(i).getId());
		}
		placementV2(testOpID, deployment);
	}


	private Map<String, AbstractSlot> allocateResourceUniformlyV2(Map<String, List<AbstractSlot>> resourceMap, int numTasks) throws Exception {
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
		// free slots from heavy nodes, and allocate slots in light nodes
		for (String nodeID : resourceMap.keySet()) {
			allocateSlotsOnOneNode(resourceMap, res, pendingStots, nodeID);
		}
		if (res.size() == numTasks) {
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
}
