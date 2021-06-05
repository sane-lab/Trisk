package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
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

public class NexmarkController extends AbstractController {
	private final Map<String, String> experimentConfig;

	public final static String TEST_OPERATOR_NAME = "trisk.reconfig.operator.name";

	public NexmarkController(ReconfigurationExecutor reconfigurationExecutor, Configuration configuration) {
		super(reconfigurationExecutor);
		experimentConfig = configuration.toMap();
	}

	@Override
	protected void defineControlAction() throws Exception {
		Thread.sleep(5000);

		String testOperatorName = experimentConfig.getOrDefault(TEST_OPERATOR_NAME, "filter");
		int testOpID = findOperatorByName(testOperatorName);
		// 5s
		Thread.sleep(5000);
		smartPlacementV2(testOpID);
	}

	private void smartPlacementV2(int testOpID) throws Exception {
		TriskWithLock planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();

		Map<Integer, String> deployment = new HashMap<>();

		Map<String, List<AbstractSlot>> resourceMap = planWithLock.getResourceDistribution();

		int p = planWithLock.getParallelism(testOpID);
		Map<String, AbstractSlot> allocatedSlots = allocateResourceUniformlyV2(resourceMap, p, p/2);
		Preconditions.checkNotNull(allocatedSlots, "no more slots can be allocated");
		// place half of tasks with new slots
		List<Integer> modifiedTasks = new ArrayList<>();
		for (int taskId = 0; taskId < p; taskId++) {
			TaskResourceDescriptor task = planWithLock.getExecutionPlan().getTaskResource(testOpID, taskId);
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

		placementV2(testOpID, deployment);
	}

}
