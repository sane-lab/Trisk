/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.controlplane.streammanager.abstraction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.controlplane.abstraction.NodeDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.TaskDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.resource.AbstractSlot;
import org.apache.flink.runtime.controlplane.abstraction.resource.FlinkSlot;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

import static org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor.ExecutionLogic.UDF;

public final class ExecutionPlanImpl implements ExecutionPlan {
	private static final Logger LOG = LoggerFactory.getLogger(ExecutionPlanImpl.class);
	// operatorId -> operator
	private final Map<Integer, OperatorDescriptor> jobConfigurations;
	@Deprecated
	// node with resources, Deprecated, we now use slotMap to represent the resource distribution.
	private final List<NodeDescriptor> resourceDistribution;

	// slots to location map, which record the available slots for streaming job
	private Map<String, List<AbstractSlot>> slotMap = new HashMap<>();

	// transformation operations -> affected tasks grouped by operators.
	private final Map<String, Map<Integer, List<Integer>>> transformations = new HashMap<>();

	private final Map<Integer, List<SlotID>> slotAllocation = new HashMap<>();

	@Internal
	public ExecutionPlanImpl(Map<Integer, OperatorDescriptor> jobConfigurations,
							 List<NodeDescriptor> resourceDistribution) {
		this.jobConfigurations = jobConfigurations;
		this.resourceDistribution = resourceDistribution;
	}

	public ExecutionPlanImpl(Map<Integer, OperatorDescriptor> jobConfigurations,
							 List<NodeDescriptor> resourceDistribution,
							 Map<String, List<AbstractSlot>> slotMap) {
		this.jobConfigurations = jobConfigurations;
		this.resourceDistribution = resourceDistribution;
		this.slotMap = slotMap;
	}

	@Override
	public int getParallelism(Integer operatorID) {
		return this.jobConfigurations.get(operatorID).getParallelism();
	}

	@Override
	public Function getUserFunction(Integer operatorID) {
		return jobConfigurations.get(operatorID).getUdf();
	}

	@Override
	public Map<Integer, List<Integer>> getKeyStateAllocation(Integer operatorID) {
		return jobConfigurations.get(operatorID).getKeyStateDistribution();
	}

	@Override
	public Map<Integer, Map<Integer, List<Integer>>> getKeyMapping(Integer operatorID) {
		return jobConfigurations.get(operatorID).getKeyMapping();
	}

	@Override
	public Iterator<OperatorDescriptor> getAllOperator() {
		return jobConfigurations.values().iterator();
	}

	@Override
	public OperatorDescriptor getOperatorByID(Integer operatorID) {
		return jobConfigurations.get(operatorID);
	}

	@Override
	public ExecutionPlan assignWorkload(Integer operatorID, Map<Integer, List<Integer>> distribution) {
		Preconditions.checkNotNull(getKeyStateAllocation(operatorID), "previous key state allocation should not be null");
		OperatorDescriptor targetDescriptor = getOperatorByID(operatorID);
		// update the key set
//		targetDescriptor.updateKeyStateAllocation(distribution);
		Map<Integer, List<Integer>> upstreamTasks = transformations.getOrDefault("upstream", new HashMap<>());
		for (OperatorDescriptor parent : targetDescriptor.getParents()) {
			parent.updateKeyMapping(operatorID, distribution);
			upstreamTasks.put(parent.getOperatorID(), parent.getTaskIds());
		}
		transformations.put("upstream", upstreamTasks);

		// update the parallelism if the distribution key size is different
		if (targetDescriptor.getParallelism() != distribution.size()) {
			// add the downstream tasks that need to know the members update in the upstream.
			Map<Integer, List<Integer>> downStreamTasks = transformations.getOrDefault("downstream", new HashMap<>());
			targetDescriptor.setParallelism(distribution.size());
			// put tasks in downstream vertex
			targetDescriptor.getChildren()
				.forEach(c -> downStreamTasks.put(c.getOperatorID(), c.getTaskIds()));
			transformations.put("downstream", downStreamTasks);
		}

		// find out affected tasks, add them to transformations
		Map<Integer, List<Integer>> updateStateTasks = transformations.getOrDefault("redistribute", new HashMap<>());
		updateStateTasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());

		transformations.put("redistribute", updateStateTasks);
		// TODO: should be upstream to be remapped
		transformations.put("remapping", updateStateTasks);
		return this;
	}

	@Override
	public ExecutionPlan assignExecutionLogic(Integer operatorID, Object function) {
		Preconditions.checkNotNull(getUserFunction(operatorID), "previous key state allocation should not be null");
		OperatorDescriptor targetDescriptor = getOperatorByID(operatorID);
		try {
			targetDescriptor.setControlAttribute(UDF, function);
		} catch (Exception e) {
			LOG.info("update function failed.", e);
		}
		// find out affected tasks, add them to transformations
		Map<Integer, List<Integer>> updateFunctionTasks = transformations.getOrDefault("updateExecutionLogic", new HashMap<>());
		updateFunctionTasks.putIfAbsent(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
		transformations.put("updateExecutionLogic", updateFunctionTasks);
		return this;
	}

	@Override
	/**
	 * @param deployment : TaskId -> Tuple2<newTaskId, SlotID>
	 */
	public ExecutionPlan assignResources(Integer operatorID, @Nullable Map<Integer, Tuple2<Integer, String>> deployment) {
		// TODO: deployment is null, default deployment, needs to assign tasks to nodes
		// TODO: deployment is nonnull, assign tasks to target Node with resources
		OperatorDescriptor targetDescriptor = getOperatorByID(operatorID);
		// need to find out which task is to be redeployed
		// because we used cancel and redeploy method, the new deployed task should have a new task id
		// the actual placement is to migrate entire state from one task to another, and then kill the former.
		Map<Integer, List<Integer>> keyDistribution = targetDescriptor.getKeyStateDistribution();
		Map<Integer, List<Integer>> newKeyDistribution = new HashMap<>();
		if (deployment != null) {
			if (deployment.size() != keyDistribution.size())
				throw new RuntimeException("++++++ inconsistent number of tasks in workload allocation and resource allocation.");

			slotAllocation.putIfAbsent(operatorID, new ArrayList<>());
			for (Integer taskId : deployment.keySet()) {
				// set a new KeyStateAllocation through the new deployment
				Tuple2<Integer, String> idAndSlots = deployment.get(taskId);
				Integer newTaskId = idAndSlots.f0;
				// todo, temporary solution for store slot allocation info
				slotAllocation.get(operatorID).add(FlinkSlot.toSlotId(idAndSlots.f1));
				newKeyDistribution.put(newTaskId, keyDistribution.get(taskId));
			}
			// update the key set
			Map<Integer, List<Integer>> upstreamTasks = transformations.getOrDefault("upstream", new HashMap<>());
			for (OperatorDescriptor parent : targetDescriptor.getParents()) {
				parent.updateKeyMapping(operatorID, newKeyDistribution);
				upstreamTasks.put(parent.getOperatorID(), parent.getTaskIds());
			}
			transformations.put("upstream", upstreamTasks);

			// add the downstream tasks that need to know the members update in the upstream.
			Map<Integer, List<Integer>> downStreamTasks = transformations.getOrDefault("downstream", new HashMap<>());
			// put tasks in downstream vertex
			targetDescriptor.getChildren()
				.forEach(c -> downStreamTasks.put(c.getOperatorID(), c.getTaskIds()));
			transformations.put("downstream", downStreamTasks);

			Map<Integer, List<Integer>> reDeployingTasks = transformations.getOrDefault("redeploying", new HashMap<>());
			reDeployingTasks.putIfAbsent(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			transformations.put("redeploying", reDeployingTasks);
			transformations.put("remapping", reDeployingTasks);
		} else {
			Map<Integer, List<Integer>> reDeployingTasks = transformations.getOrDefault("redeploying", new HashMap<>());
			reDeployingTasks.putIfAbsent(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			transformations.put("redeploying", reDeployingTasks);
		}
		return this;
	}

	@Override
	/**
	 * @param deployment : TaskId -> Tuple2<newTaskId, SlotID>
	 */
	public ExecutionPlan assignResourcesV2(Integer operatorID, @Nullable Map<Integer, String> deployment) {
		OperatorDescriptor targetDescriptor = getOperatorByID(operatorID);
		// need to find out which task is to be redeployed
		// because we used cancel and redeploy method, the new deployed task should have a new task id
		// the actual placement is to migrate entire state from one task to another, and then kill the former.
		Map<Integer, List<Integer>> keyDistribution = targetDescriptor.getKeyStateDistribution();
		Map<Integer, Tuple2<Integer, String>> convertedDeployment = new HashMap<>();

		Map<Integer, List<Integer>> newKeyDistribution = new HashMap<>();
		if (deployment != null) {
			int p = keyDistribution.size();
			// todo, temporary solution for store slot allocation info
			slotAllocation.computeIfAbsent(operatorID, k -> new ArrayList<>());
			for (int taskId : deployment.keySet()) {
				String curSlotId = deployment.get(taskId);
				// if the current slotId is not equal to the slot id in the existing task
				if (targetDescriptor.getTask(taskId).resourceSlot.equals(curSlotId)) {
					convertedDeployment.put(taskId, Tuple2.of(taskId, curSlotId));
				} else {
					convertedDeployment.put(taskId, Tuple2.of(taskId + p, curSlotId));
					slotAllocation.get(operatorID).add(FlinkSlot.toSlotId(curSlotId));
				}
			}

			if (convertedDeployment.size() != keyDistribution.size())
				throw new RuntimeException("++++++ inconsistent number of tasks in workload allocation and resource allocation.");

			for (Integer taskId : convertedDeployment.keySet()) {
				// set a new KeyStateAllocation through the new deployment
				Tuple2<Integer, String> idAndSlots = convertedDeployment.get(taskId);
				Integer newTaskId = idAndSlots.f0;
//				slotAllocation.get(operatorID).add(FlinkSlot.toSlotId(idAndSlots.f1));
				newKeyDistribution.put(newTaskId, keyDistribution.get(taskId));
			}
			// update the key set
			Map<Integer, List<Integer>> upstreamTasks = transformations.getOrDefault("upstream", new HashMap<>());
			for (OperatorDescriptor parent : targetDescriptor.getParents()) {
				parent.updateKeyMapping(operatorID, newKeyDistribution);
				upstreamTasks.put(parent.getOperatorID(), parent.getTaskIds());
			}
			transformations.put("upstream", upstreamTasks);

			// add the downstream tasks that need to know the members update in the upstream.
			Map<Integer, List<Integer>> downStreamTasks = transformations.getOrDefault("downstream", new HashMap<>());
			// put tasks in downstream vertex
			targetDescriptor.getChildren()
				.forEach(c -> downStreamTasks.put(c.getOperatorID(), c.getTaskIds()));
			transformations.put("downstream", downStreamTasks);

			Map<Integer, List<Integer>> reDeployingTasks = transformations.getOrDefault("redeploying", new HashMap<>());
			reDeployingTasks.putIfAbsent(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			transformations.put("redeploying", reDeployingTasks);
			transformations.put("remapping", reDeployingTasks);
		} else {
			Map<Integer, List<Integer>> reDeployingTasks = transformations.getOrDefault("redeploying", new HashMap<>());
			reDeployingTasks.putIfAbsent(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			transformations.put("redeploying", reDeployingTasks);
		}
		return this;
	}

	@Override
	// users need to implement their own update executionplan + add transformations
	public ExecutionPlan update(java.util.function.Function<ExecutionPlan, ExecutionPlan> applier) {
		return applier.apply(this);
	}

	@Override
	public Map<String, Map<Integer, List<Integer>>> getTransformations() {
		return transformations;
	}

	@Override
	public Map<Integer, List<SlotID>> getSlotAllocation() {
		return slotAllocation;
	}

	@Override
	public void clearTransformations() {
		transformations.clear();
		slotAllocation.clear();
	}

//	public OperatorDescriptor[] getHeadOperators() {
//		return headOperators;
//	}

	@Override
	public List<NodeDescriptor> getResourceDistribution() {
		return resourceDistribution;
	}

	@Override
	public Map<String, List<AbstractSlot>> getSlotMap() {
		return Collections.unmodifiableMap(slotMap);
	}

	@Override
	public TaskDescriptor getTask(Integer operatorID, int taskId) {
		return jobConfigurations.get(operatorID).getTask(taskId);
//		return operatorToTaskMap.get(operatorID).get(taskId);
	}

	public void setSlotMap(Map<String, List<AbstractSlot>> slotMap) {
		this.slotMap = slotMap;
	}

	public ExecutionPlan copy() {
		List<NodeDescriptor> resourceDistributionCopy = new ArrayList<>();
		for (NodeDescriptor node : resourceDistribution) {
			NodeDescriptor nodeCopy = node.copy();
			resourceDistributionCopy.add(nodeCopy);
		}

		Map<String, List<AbstractSlot>> slotMapCopy = new HashMap<>();
		for (String location : slotMap.keySet()) {
			List<AbstractSlot> slots = slotMap.get(location);
			List<AbstractSlot> slotsCopy = new ArrayList<>();
			for (AbstractSlot slot : slots) {
				AbstractSlot slotCopy = slot.copy();
				slotsCopy.add(slotCopy);
			}
			slotMapCopy.put(location, slotsCopy);
		}

		Map<Integer, OperatorDescriptor> jobConfigurationsCopy = new HashMap<>();
		for (Integer operatorID : jobConfigurations.keySet()) {
			OperatorDescriptor operatorDescriptor = jobConfigurations.get(operatorID);
			OperatorDescriptor operatorDescriptorCopy = operatorDescriptor.copy(resourceDistributionCopy);
			jobConfigurationsCopy.put(operatorID, operatorDescriptorCopy);
		}

		// construct the DAG
		for (Integer operatorID : jobConfigurations.keySet()) {
			OperatorDescriptor operatorDescriptor = jobConfigurations.get(operatorID);
			OperatorDescriptor operatorDescriptorCopy = jobConfigurationsCopy.get(operatorID);
			for (OperatorDescriptor upstream : operatorDescriptor.getParents()) {
				OperatorDescriptor upstreamCopy = jobConfigurationsCopy.get(upstream.getOperatorID());
				operatorDescriptorCopy.addParent(upstreamCopy);
			}
			for (OperatorDescriptor downstream : operatorDescriptor.getChildren()) {
				OperatorDescriptor downstreamCopy = jobConfigurationsCopy.get(downstream.getOperatorID());
				operatorDescriptorCopy.addChildren(downstreamCopy);
			}
		}

		return new ExecutionPlanImpl(jobConfigurationsCopy, resourceDistributionCopy, slotMapCopy);
	}
}
