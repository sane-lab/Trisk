package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.TaskResourceDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.resource.AbstractSlot;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.TriskWithLock;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.apache.flink.util.Preconditions;

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
	protected void defineControlAction() throws Exception {
		Thread.sleep(10 * 1000);
		requestTime += 2 * 60;
		Thread.sleep(10 * 1000);
		smartPlacementV2(findOperatorByName("dtree"));
		Thread.sleep(10 * 1000);
	}

	private void smartPlacementV2(int testOpID) throws Exception {
		TriskWithLock planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
		Map<Integer, String> deployment = new HashMap<>();
		Map<String, List<AbstractSlot>> resourceMap = planWithLock.getResourceDistribution();
		OperatorDescriptor operatorDescriptor = planWithLock.getOperatorByID(testOpID);
		int p = planWithLock.getParallelism(testOpID);
		Map<String, AbstractSlot> allocatedSlots = allocateResourceUniformlyV2(resourceMap, p, p/2);
		Preconditions.checkNotNull(allocatedSlots, "no more slots can be allocated");
		// place half of tasks with new slots
		List<Integer> modifiedTasks = new ArrayList<>();
		for (int taskId = 0; taskId < p; taskId++) {
			TaskResourceDescriptor task = operatorDescriptor.getTaskResource(taskId);
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

}
