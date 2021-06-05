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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.TaskResourceDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.resource.AbstractSlot;
import org.apache.flink.streaming.controlplane.udm.ControlPolicy;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The state of stream manager mainly contains the following information:
 * 1. topology: the topology of the whole dataflow,
 * 2. key mappings: mappings of upstream key groups to downstream tasks,
 * 3. key state allocation: key groups allocation among tasks in the same stage,
 * 4. user defined function:user defined execution logic of each task,
 * 5. nThreads: num-ber of threads owned by each task,
 * 6. task location: location of one task in cluster.
 * <p>
 * G(V,E) is a graph with a set of vertices V connected by a set of edges E.
 * G(V,E) describes the operator level abstraction.
 * V contains the execution logic configurations: user defined function and key state allocation.
 * E provides the connectivity information of different vertices, the main information is keymappings.
 * <p>
 * D(H,T) is the deployment of tasks of the streaming job on the cluster, it describes task level abstraction.
 * H represents the hosts in the cluster, each host has a certain number of CPU and memory resources.
 * T is the set of tasks, the main information in T is: number of threads owned by each task and task location.
 */
public class TriskWithLock {
//	implements TriskWithLock {

	private final static int COMMITTED = 1;
	private final static int STAGED = 0;

	private final AtomicInteger stateOfUpdate = new AtomicInteger(COMMITTED);
	private ControlPolicy currentWaitingController;

	private final ExecutionPlan executionPlan;

	public TriskWithLock(ExecutionPlan executionPlan) {
		this.executionPlan = executionPlan;
	}

	private TriskWithLock(ExecutionPlan executionPlan, AtomicInteger stateOfUpdate, ControlPolicy currentWaitingController) {
		this.executionPlan = executionPlan;
		this.stateOfUpdate.set(stateOfUpdate.get());
		this.currentWaitingController = currentWaitingController;
	}

	public void setStateUpdatingFlag(ControlPolicy waitingController) throws Exception {
		// some strategy needed here to ensure there is only one update at one time
		if (!stateOfUpdate.compareAndSet(COMMITTED, STAGED)) {
			throw new Exception("There is another state update not finished, the waiting controller is:" + currentWaitingController);
		}
		// the caller may want to wait the completion of this update.
		currentWaitingController = waitingController;
	}

	public void notifyUpdateFinished(Throwable throwable) throws Exception {
		if (stateOfUpdate.compareAndSet(STAGED, COMMITTED)) {
			if (currentWaitingController != null) {
				currentWaitingController.onChangeCompleted(throwable);
			}
			return;
		}
		throw new Exception("There is not any state updating");
	}

	public ExecutionPlan getExecutionPlan() {
		return executionPlan;
	}

	// delegate methods
	public Map<String, List<AbstractSlot>> getResourceDistribution() {
		return executionPlan.getSlotMap();
	}

	public TaskResourceDescriptor getTask(Integer operatorID, int taskId) {
		return executionPlan.getTaskResource(operatorID, taskId);
	}

	public Function getUserFunction(Integer operatorID) {
		return executionPlan.getUserFunction(operatorID);
	}

	public Map<Integer, List<Integer>> getKeyDistribution(Integer operatorID){
		return executionPlan.getKeyStateAllocation(operatorID);
	}

	public Map<Integer, Map<Integer, List<Integer>>> getKeyMapping(Integer operatorID) {
		return executionPlan.getKeyMapping(operatorID);
	}

	public int getParallelism(Integer operatorID) {
		return executionPlan.getParallelism(operatorID);
	}

	public Iterator<OperatorDescriptor> getAllOperator() {
		return executionPlan.getAllOperator();
	}

	public OperatorDescriptor getOperatorByID(Integer operatorID) {
		return executionPlan.getOperatorByID(operatorID);
	}

	public ExecutionPlan assignWorkload(Integer operatorID, Map<Integer, List<Integer>> distribution) {
		return executionPlan.assignWorkload(operatorID, distribution);
	}

	public ExecutionPlan assignExecutionLogic(Integer operatorID, Object function) {
		return executionPlan.assignExecutionLogic(operatorID, function);
	}

	public ExecutionPlan assignResources(Integer operatorID, @Nullable Map<Integer, Tuple2<Integer, String>> deployment) {
		return executionPlan.assignResources(operatorID, deployment);
	}

	public ExecutionPlan assignResourcesV2(Integer operatorID, @Nullable Map<Integer, String> deployment) {
		return executionPlan.assignResourcesV2(operatorID, deployment);
	}

//	@Override
	public ExecutionPlan update(java.util.function.Function<ExecutionPlan, ExecutionPlan> applier) {
		return executionPlan.update(applier);
	}

	public Map<Integer, List<SlotID>> getSlotAllocation() {
		return executionPlan.getSlotAllocation();
	}

	public Map<String, Map<Integer, List<Integer>>> getTransformations() {
		return executionPlan.getTransformations();
	}

	public void clearTransformations() {
		executionPlan.clearTransformations();
	}

    public TriskWithLock copy() {
		return new TriskWithLock(executionPlan.copy(), stateOfUpdate, currentWaitingController);
    }

}
