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

package org.apache.flink.runtime.rescale;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.rescale.streamswitch.FlinkStreamSwitchAdaptor;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class JobRescaleCoordinator implements JobRescaleAction, RescalepointAcknowledgeListener {

	private static final Logger LOG = LoggerFactory.getLogger(JobRescaleCoordinator.class);

	private final JobGraph jobGraph;

	private ExecutionGraph executionGraph;

	private ComponentMainThreadExecutor mainThreadExecutor;

	private FlinkStreamSwitchAdaptor streamSwitchAdaptor;

	private final JobGraphRescaler jobGraphRescaler;

	private final List<ExecutionAttemptID> notYetAcknowledgedTasks;

	private JobStatusListener jobStatusListener;

	private final Object lock = new Object();

	// mutable fields
	private volatile boolean inProcess;

	private volatile ActionType actionType;

	private volatile RescaleID rescaleId;

	private volatile JobRescalePartitionAssignment jobRescalePartitionAssignment;

	private volatile ExecutionJobVertex targetVertex;

	private volatile long checkpointId;

	// mutable fields for scale in/out
	private volatile List<ExecutionVertex> createCandidates;

	private volatile List<ExecutionVertex> removedCandidates;

	private volatile Collection<Execution> allocatedExecutions;


	public JobRescaleCoordinator(
			JobGraph jobGraph,
			ExecutionGraph executionGraph,
			ClassLoader userCodeLoader) {

		this.jobGraph = jobGraph;
		this.executionGraph = executionGraph;

		this.notYetAcknowledgedTasks = new ArrayList<>();

		this.streamSwitchAdaptor = new FlinkStreamSwitchAdaptor(this, executionGraph);
		this.jobGraphRescaler = JobGraphRescaler.instantiate(jobGraph, userCodeLoader);
	}

	public void init(ComponentMainThreadExecutor mainThreadExecutor) {
		this.mainThreadExecutor = mainThreadExecutor;
	}

	public void start() {
		streamSwitchAdaptor.startControllers();
	}

	public void stop() {
		streamSwitchAdaptor.stopControllers();
	}

	public void assignExecutionGraph(ExecutionGraph executionGraph) {
		checkState(!inProcess, "ExecutionGraph changed after rescaling starts");
		this.executionGraph = executionGraph;

		streamSwitchAdaptor.stopControllers();
		this.streamSwitchAdaptor = new FlinkStreamSwitchAdaptor(this, executionGraph);

		streamSwitchAdaptor.startControllers();
	}

	@Override
	public JobGraph getJobGraph() {
		return this.jobGraph;
	}

	@Override
	public void repartition(JobVertexID vertexID, JobRescalePartitionAssignment jobRescalePartitionAssignment) {
		checkState(!inProcess, "Current rescaling hasn't finished.");
		inProcess = true;
		actionType = ActionType.REPARTITION;

		rescaleId = RescaleID.generateNextID();
		this.jobRescalePartitionAssignment = jobRescalePartitionAssignment;

		LOG.info("++++++ repartition job with RescaleID: " + rescaleId +
			", taskID" + vertexID +
			", partitionAssignment: " + jobRescalePartitionAssignment);

		List<JobVertexID> involvedUpstream = new ArrayList<>();
		List<JobVertexID> involvedDownstream = new ArrayList<>();
		try {
			jobGraphRescaler.repartition(vertexID,
				jobRescalePartitionAssignment.getPartitionAssignment(),
				involvedUpstream, involvedDownstream);
			executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));

			repartitionVertex(vertexID, involvedUpstream, involvedDownstream);
		} catch (Exception e) {
			failExecution(e);
		}
	}

	@Override
	public void scaleOut(JobVertexID vertexID, int parallelism, JobRescalePartitionAssignment jobRescalePartitionAssignment) {
		checkState(!inProcess, "Current rescaling hasn't finished.");
		inProcess = true;
		actionType = ActionType.SCALE_OUT;

		rescaleId = RescaleID.generateNextID();
		this.jobRescalePartitionAssignment = jobRescalePartitionAssignment;
		LOG.info("++++++ scale out job with RescaleID: " + rescaleId +
			", taskID" + vertexID +
			", new parallelism: " + parallelism +
			", partitionAssignment: " + jobRescalePartitionAssignment);

		List<JobVertexID> involvedUpstream = new ArrayList<>();
		List<JobVertexID> involvedDownstream = new ArrayList<>();
		try {
			jobGraphRescaler.rescale(vertexID, parallelism,
				jobRescalePartitionAssignment.getPartitionAssignment(),
				involvedUpstream, involvedDownstream);
			executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));

			scaleOutVertex(vertexID, involvedUpstream, involvedDownstream);
		} catch (Exception e) {
			failExecution(e);
		}
	}

	@Override
	public void scaleIn(JobVertexID vertexID, int parallelism, JobRescalePartitionAssignment jobRescalePartitionAssignment) {
		checkState(!inProcess, "Current rescaling hasn't finished.");
		inProcess = true;
		actionType = ActionType.SCALE_IN;

		rescaleId = RescaleID.generateNextID();
		this.jobRescalePartitionAssignment = jobRescalePartitionAssignment;
		LOG.info("++++++ scale in job with RescaleID: " + rescaleId + ", new parallelism: " + parallelism);

		List<JobVertexID> involvedUpstream = new ArrayList<>();
		List<JobVertexID> involvedDownstream = new ArrayList<>();
		try {
			jobGraphRescaler.rescale(vertexID, parallelism,
				jobRescalePartitionAssignment.getPartitionAssignment(),
				involvedUpstream, involvedDownstream);
			executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));

			scaleInVertex(vertexID, involvedUpstream, involvedDownstream);
		} catch (Exception e) {
			failExecution(e);
		}
	}

	private void repartitionVertex(
			JobVertexID vertexID,
			List<JobVertexID> updatedUpstream,
			List<JobVertexID> updatedDownstream) throws ExecutionGraphException {

		Map<JobVertexID, ExecutionJobVertex> tasks = executionGraph.getAllVertices();

		CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

		checkNotNull(checkpointCoordinator);
		checkpointCoordinator.setRescalepointAcknowledgeListener(this);

		this.targetVertex = tasks.get(vertexID);

		for (ExecutionVertex vertex : this.targetVertex.getTaskVertices()) {
			notYetAcknowledgedTasks.add(vertex.getCurrentExecutionAttempt().getAttemptId());
		}

		// state check
		checkState(targetVertex.getParallelism() == jobRescalePartitionAssignment.getNumOpenedSubtask(),
			String.format("parallelism in targetVertex %d is not equal to number of executors %d",
				targetVertex.getParallelism(), jobRescalePartitionAssignment.getNumOpenedSubtask()));

		// rescale upstream and downstream
		final Collection<CompletableFuture<Void>> rescaleCandidatesFutures = new ArrayList<>();

		for (JobVertexID jobId : updatedUpstream) {
			tasks.get(jobId).cleanBeforeRescale();

			for (ExecutionVertex vertex : tasks.get(jobId).getTaskVertices()) {
				Execution execution = vertex.getCurrentExecutionAttempt();
				rescaleCandidatesFutures.add(execution.scheduleRescale(rescaleId, RescaleOptions.RESCALE_PARTITIONS_ONLY, null));
			}
		}

		for (JobVertexID jobId : updatedDownstream) {
			tasks.get(jobId).cleanBeforeRescale();

			for (ExecutionVertex vertex : tasks.get(jobId).getTaskVertices()) {
				Execution execution = vertex.getCurrentExecutionAttempt();
				notYetAcknowledgedTasks.add(execution.getAttemptId());
				rescaleCandidatesFutures.add(execution.scheduleRescale(rescaleId, RescaleOptions.RESCALE_GATES_ONLY, null));
			}
		}

		for (int subtaskIndex = 0; subtaskIndex < targetVertex.getTaskVertices().length; subtaskIndex++) {
			if (!jobRescalePartitionAssignment.isSubtaskModified(subtaskIndex)) {
				ExecutionVertex vertex = targetVertex.getTaskVertices()[subtaskIndex];
				Execution execution = vertex.getCurrentExecutionAttempt();

				rescaleCandidatesFutures.add(
					execution.scheduleRescale(rescaleId, RescaleOptions.RESCALE_BOTH, null));
			}
		}

		FutureUtils
			.combineAll(rescaleCandidatesFutures)
			.whenComplete((ignored, failure) -> {
				if (failure != null) {
					failExecution(failure);
					throw new CompletionException(failure);
				}
				LOG.info("++++++ Rescale vertex Completed");
			})
			.thenRunAsync(() -> {
				try {
					checkpointCoordinator.stopCheckpointScheduler();
					checkpointCoordinator.triggerRescalePoint(System.currentTimeMillis())
						.whenComplete((completedCheckpoint, throwable) -> {
							if (throwable == null) {
								LOG.info("++++++ Make rescalepoint with checkpointId=" + completedCheckpoint.getCheckpointID());
							}
						});
				} catch (Exception e) {
					failExecution(e);
					throw new CompletionException(e);
				}
			}, mainThreadExecutor);
	}

	private void scaleOutVertex(
			JobVertexID vertexID,
			List<JobVertexID> updatedUpstream,
			List<JobVertexID> updatedDownstream) {

		Map<JobVertexID, ExecutionJobVertex> tasks = executionGraph.getAllVertices();

		CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

		checkNotNull(checkpointCoordinator);
		checkpointCoordinator.setRescalepointAcknowledgeListener(this);

		this.targetVertex = tasks.get(vertexID);

		for (ExecutionVertex vertex : this.targetVertex.getTaskVertices()) {
			notYetAcknowledgedTasks.add(vertex.getCurrentExecutionAttempt().getAttemptId());
		}

		final Map<RescaleOptions, List<ExecutionVertex>> rescaleCandidates = new HashMap<>();

		rescaleCandidates.put(RescaleOptions.RESCALE_PARTITIONS_ONLY, new ArrayList<>());
		rescaleCandidates.put(RescaleOptions.RESCALE_GATES_ONLY, new ArrayList<>());
//		rescaleCandidates.put(RescaleOptions.RESCALE_BOTH, new ArrayList<>());

		for (JobVertexID jobId : updatedUpstream) {
			tasks.get(jobId).cleanBeforeRescale();

			rescaleCandidates
				.get(RescaleOptions.RESCALE_PARTITIONS_ONLY)
				.addAll(Arrays.asList(tasks.get(jobId).getTaskVertices()));
		}

//		rescaleCandidates
//			.get(RescaleOptions.RESCALE_BOTH)
//			.addAll(Arrays.asList(this.targetVertex.getTaskVertices()));

		for (JobVertexID jobId : updatedDownstream) {
			tasks.get(jobId).cleanBeforeRescale();

			rescaleCandidates
				.get(RescaleOptions.RESCALE_GATES_ONLY)
				.addAll(Arrays.asList(tasks.get(jobId).getTaskVertices()));

			for (ExecutionVertex vertex : tasks.get(jobId).getTaskVertices()) {
				notYetAcknowledgedTasks.add(vertex.getCurrentExecutionAttempt().getAttemptId());
			}
		}

		// scale up given ejv, update involved edges & partitions
		this.createCandidates = this.targetVertex.scaleOut(executionGraph.getRpcTimeout(), executionGraph.getGlobalModVersion(), System.currentTimeMillis());

		for (JobVertexID downstreamID : updatedDownstream) {
			ExecutionJobVertex downstream = tasks.get(downstreamID);
			downstream.reconnectWithUpstream(this.targetVertex.getProducedDataSets());
		}
		executionGraph.updateNumOfTotalVertices();

		// state check
		checkState(targetVertex.getParallelism() == jobRescalePartitionAssignment.getNumOpenedSubtask(),
			String.format("parallelism in targetVertex %d is not equal to number of executors %d",
				targetVertex.getParallelism(), jobRescalePartitionAssignment.getNumOpenedSubtask()));

		// required resource for all created vertices
		Collection<CompletableFuture<Execution>> allocateSlotFutures = new ArrayList<>(this.createCandidates.size());

		for (ExecutionVertex vertex : this.createCandidates) {
			Execution executionAttempt = vertex.getCurrentExecutionAttempt();
			allocateSlotFutures.add(executionAttempt.allocateAndAssignSlotForExecution(rescaleId));
		}

		// rescale existed vertices from upstream to downstream
		CompletableFuture<Void> rescaleCompleted = FutureUtils
			.combineAll(allocateSlotFutures)
			.whenComplete((executions, failure) -> {
				if (failure != null) {
					failExecution(failure);
					throw new CompletionException(failure);
				}
				LOG.info("++++++ allocate resource for vertices Completed");

				allocatedExecutions = executions;
			})
			.thenApplyAsync((ignored) -> {
				try {
					Collection<CompletableFuture<Void>> rescaleCandidatesFutures = new ArrayList<>();

					for (Map.Entry<RescaleOptions, List<ExecutionVertex>> entry : rescaleCandidates.entrySet()) {
						for (ExecutionVertex vertex : entry.getValue()) {
							Execution execution = vertex.getCurrentExecutionAttempt();
							rescaleCandidatesFutures.add(execution.scheduleRescale(rescaleId, entry.getKey(), null));
						}
					}

					for (int subtaskIndex = 0; subtaskIndex < targetVertex.getTaskVertices().length; subtaskIndex++) {
						if (!jobRescalePartitionAssignment.isSubtaskModified(subtaskIndex)) {
							ExecutionVertex vertex = targetVertex.getTaskVertices()[subtaskIndex];
							Execution execution = vertex.getCurrentExecutionAttempt();

							rescaleCandidatesFutures.add(execution.scheduleRescale(rescaleId, RescaleOptions.RESCALE_BOTH, null));
						}
					}

					return FutureUtils
						.completeAll(rescaleCandidatesFutures)
						.whenComplete((ignored2, failure) -> {
							if (failure != null) {
								failExecution(failure);
								throw new CompletionException(failure);
							}
							LOG.info("++++++ Rescale vertex Completed");
						});
				} catch (Exception cause) {
					failExecution(cause);
					throw new CompletionException(cause);
				}
			}, mainThreadExecutor)
			.thenCompose(Function.identity());

		// trigger rescale point
		rescaleCompleted
			.thenRunAsync(() -> {
				try {
					checkpointCoordinator.stopCheckpointScheduler();
					checkpointCoordinator.triggerRescalePoint(System.currentTimeMillis())
						.whenComplete((completedCheckpoint, throwable) -> {
							if (throwable == null) {
								LOG.info("++++++ Make rescalepoint with checkpointId=" + completedCheckpoint.getCheckpointID());
							}
						});
				} catch (Exception e) {
					failExecution(e);
					throw new CompletionException(e);
				}
			}, mainThreadExecutor);
	}

	private void scaleInVertex(
			JobVertexID vertexID,
			List<JobVertexID> updatedUpstream,
			List<JobVertexID> updatedDownstream) throws ExecutionGraphException {

		Map<JobVertexID, ExecutionJobVertex> tasks = executionGraph.getAllVertices();

		CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

		checkNotNull(checkpointCoordinator);
		checkpointCoordinator.setRescalepointAcknowledgeListener(this);

		this.targetVertex = tasks.get(vertexID);

		for (ExecutionVertex vertex : this.targetVertex.getTaskVertices()) {
			notYetAcknowledgedTasks.add(vertex.getCurrentExecutionAttempt().getAttemptId());
		}

		// scale in by given ejv, update involved edges & partitions
		this.removedCandidates = this.targetVertex.scaleIn(executionGraph.getRpcTimeout(), executionGraph.getGlobalModVersion(), System.currentTimeMillis());

		for (JobVertexID upstreamID : updatedUpstream) {
			ExecutionJobVertex upstream = tasks.get(upstreamID);
			upstream.resetProducedDataSets();
			targetVertex.reconnectWithUpstream(upstream.getProducedDataSets());
		}


		for (JobVertexID downstreamID : updatedDownstream) {
			ExecutionJobVertex downstream = tasks.get(downstreamID);
			downstream.reconnectWithUpstream(this.targetVertex.getProducedDataSets());
		}
		executionGraph.updateNumOfTotalVertices();

		// rescale upstream and downstream
		final Collection<CompletableFuture<Void>> rescaleCandidatesFutures = new ArrayList<>();

		for (JobVertexID jobId : updatedUpstream) {
			tasks.get(jobId).cleanBeforeRescale();

			for (ExecutionVertex vertex : tasks.get(jobId).getTaskVertices()) {
				Execution execution = vertex.getCurrentExecutionAttempt();
				rescaleCandidatesFutures.add(execution.scheduleRescale(rescaleId, RescaleOptions.RESCALE_PARTITIONS_ONLY, null));
			}
		}

		for (JobVertexID jobId : updatedDownstream) {
			tasks.get(jobId).cleanBeforeRescale();

			for (ExecutionVertex vertex : tasks.get(jobId).getTaskVertices()) {
				Execution execution = vertex.getCurrentExecutionAttempt();
				notYetAcknowledgedTasks.add(execution.getAttemptId());
				rescaleCandidatesFutures.add(execution.scheduleRescale(rescaleId, RescaleOptions.RESCALE_GATES_ONLY, null));
			}
		}

		FutureUtils
			.combineAll(rescaleCandidatesFutures)
			.whenComplete((ignored, failure) -> {
				if (failure != null) {
					failExecution(failure);
					throw new CompletionException(failure);
				}
				LOG.info("++++++ Rescale vertex Completed");
			})
			.thenRunAsync(() -> {
				try {
					checkpointCoordinator.stopCheckpointScheduler();
					checkpointCoordinator.triggerRescalePoint(System.currentTimeMillis())
						.whenComplete((completedCheckpoint, throwable) -> {
							if (throwable == null) {
								LOG.info("++++++ Make rescalepoint with checkpointId=" + completedCheckpoint.getCheckpointID());
							}
						});
				} catch (Exception e) {
					failExecution(e);
					throw new CompletionException(e);
				}
			}, mainThreadExecutor);
	}

	private void handleCollectedStates(Map<OperatorID, OperatorState> operatorStates) throws Exception {
		switch (actionType) {
			case REPARTITION:
				assignNewState(operatorStates);
				break;
			case SCALE_OUT:
				deployCreatedExecution(operatorStates);
				break;
			case SCALE_IN:
				cancelOldExecution(operatorStates);
				break;
			default:
				throw new IllegalStateException("illegal action type");
		}
	}

	private void assignNewState(Map<OperatorID, OperatorState> operatorStates) throws ExecutionGraphException {
		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(targetVertex.getJobVertexId(), targetVertex);

		Set<ExecutionJobVertex> newTasks = new HashSet<>();
		newTasks.add(targetVertex);

		StateAssignmentOperation stateAssignmentOperation =
			new StateAssignmentOperation(checkpointId, newTasks, operatorStates, true);
		stateAssignmentOperation.setForceRescale(true);
		stateAssignmentOperation.setRedistributeStrategy(jobRescalePartitionAssignment);

		LOG.info("++++++ start to assign states");
		stateAssignmentOperation.assignStates();

		Collection<CompletableFuture<Void>> rescaledFuture = new ArrayList<>(targetVertex.getTaskVertices().length);

		for (int i = 0; i < targetVertex.getTaskVertices().length; i++) {
			ExecutionVertex vertex  = targetVertex.getTaskVertices()[i];
			Execution executionAttempt = vertex.getCurrentExecutionAttempt();

			CompletableFuture<Void> scheduledRescale;

			if (jobRescalePartitionAssignment.isSubtaskModified(i)) {
				scheduledRescale = executionAttempt.scheduleRescale(rescaleId,
					RescaleOptions.RESCALE_REDISTRIBUTE,
					jobRescalePartitionAssignment.getAlignedKeyGroupRange(i),
					jobRescalePartitionAssignment.getIdInModel(i));

			} else {
				scheduledRescale = executionAttempt.scheduleRescale(rescaleId,
					RescaleOptions.RESCALE_KEYGROUP_RANGE_ONLY,
					jobRescalePartitionAssignment.getAlignedKeyGroupRange(i));
			}

			rescaledFuture.add(scheduledRescale);
		}
		LOG.info("++++++ Assign new state futures created");

		FutureUtils
			.combineAll(rescaledFuture)
			.thenRunAsync(() -> {
				LOG.info("++++++ Assign new state for repartition Completed");
				CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

				checkNotNull(checkpointCoordinator);
				if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
					checkpointCoordinator.startCheckpointScheduler();
				}

				clean();

				// notify streamSwitch that change is finished
				streamSwitchAdaptor.onMigrationExecutorsStopped(targetVertex.getJobVertexId());
				streamSwitchAdaptor.onChangeImplemented(targetVertex.getJobVertexId());
			}, mainThreadExecutor);
	}

	private void deployCreatedExecution(Map<OperatorID, OperatorState> operatorStates) throws JobException, ExecutionGraphException {
		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(targetVertex.getJobVertexId(), targetVertex);

		Set<ExecutionJobVertex> newTasks = new HashSet<>();
		newTasks.add(targetVertex);

		StateAssignmentOperation stateAssignmentOperation =
			new StateAssignmentOperation(checkpointId, newTasks, operatorStates, true);
		stateAssignmentOperation.setForceRescale(true);
		stateAssignmentOperation.setRedistributeStrategy(jobRescalePartitionAssignment);

		LOG.info("++++++ start to assign states");
		stateAssignmentOperation.assignStates();

		System.out.println("target vertex: " + targetVertex.getTaskVertices().length + " allocated vertex: " + allocatedExecutions.size());

		// update existed tasks state
		Collection<CompletableFuture<Void>> rescaledFuture = new ArrayList<>(targetVertex.getTaskVertices().length - allocatedExecutions.size());

		for (int i = 0; i < targetVertex.getTaskVertices().length; i++) {
			ExecutionVertex vertex = targetVertex.getTaskVertices()[i];
			KeyGroupRange keyGroupRange = jobRescalePartitionAssignment.getAlignedKeyGroupRange(i);

			CompletableFuture<Void> scheduledRescale;

			if (jobRescalePartitionAssignment.isSubtaskModified(i)) {
				int idInModel = jobRescalePartitionAssignment.getIdInModel(i);

				if (createCandidates.contains(vertex)) {
					scheduledRescale = vertex.getCurrentExecutionAttempt().deploy(keyGroupRange, idInModel);
				} else {
					Execution executionAttempt = vertex.getCurrentExecutionAttempt();

					scheduledRescale = executionAttempt.scheduleRescale(rescaleId,
						RescaleOptions.RESCALE_REDISTRIBUTE,
						keyGroupRange, idInModel);
				}
			} else {
				Execution executionAttempt = vertex.getCurrentExecutionAttempt();

				scheduledRescale = executionAttempt.scheduleRescale(rescaleId,
					RescaleOptions.RESCALE_KEYGROUP_RANGE_ONLY,
					keyGroupRange);
			}

			rescaledFuture.add(scheduledRescale);
		}

		FutureUtils
			.combineAll(rescaledFuture)
			.thenRunAsync(() -> {
				LOG.info("++++++ Deploy vertex and rescale existing vertices completed");
				CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

				checkNotNull(checkpointCoordinator);
				checkpointCoordinator.addVertices(createCandidates.toArray(new ExecutionVertex[0]), false);

				if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
					checkpointCoordinator.startCheckpointScheduler();
				}

				clean();

				// notify streamSwitch that change is finished
				streamSwitchAdaptor.onMigrationExecutorsStopped(targetVertex.getJobVertexId());
				streamSwitchAdaptor.onChangeImplemented(targetVertex.getJobVertexId());
			}, mainThreadExecutor);
	}

	private void cancelOldExecution(Map<OperatorID, OperatorState> operatorStates) throws ExecutionGraphException {
		Map<JobVertexID, ExecutionJobVertex> tasks = new HashMap<>();
		tasks.put(targetVertex.getJobVertexId(), targetVertex);

		Set<ExecutionJobVertex> newTasks = new HashSet<>();
		newTasks.add(targetVertex);

		StateAssignmentOperation stateAssignmentOperation =
			new StateAssignmentOperation(checkpointId, newTasks, operatorStates, true);
		stateAssignmentOperation.setForceRescale(true);
		stateAssignmentOperation.setRedistributeStrategy(jobRescalePartitionAssignment);

		LOG.info("++++++ start to assign states");
		stateAssignmentOperation.assignStates();

		Collection<CompletableFuture<Void>> rescaledFuture = new ArrayList<>(targetVertex.getTaskVertices().length);
		Map<Integer, List<Integer>> partitionAssignment = jobRescalePartitionAssignment.getPartitionAssignment();

		for (int i = 0; i < targetVertex.getTaskVertices().length; i++) {
			ExecutionVertex vertex  = targetVertex.getTaskVertices()[i];
			Execution executionAttempt = vertex.getCurrentExecutionAttempt();

			CompletableFuture<Void> scheduledRescale;

			if (partitionAssignment.get(i).size() == 0) {
				System.out.println("none keygroup assigned for current jobvertex: " + vertex.toString());
				scheduledRescale = executionAttempt.scheduleRescale(rescaleId,
					RescaleOptions.RESCALE_REDISTRIBUTE, null);
			} else {
				scheduledRescale = executionAttempt.scheduleRescale(rescaleId,
					RescaleOptions.RESCALE_REDISTRIBUTE,
					jobRescalePartitionAssignment.getAlignedKeyGroupRange(i));
			}
			rescaledFuture.add(scheduledRescale);
		}
		LOG.info("++++++ Assign new state futures created");

		FutureUtils
			.combineAll(rescaledFuture)
			.thenRunAsync(() -> {
				LOG.info("++++++ Scale in and assign new state Completed");
				CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();

				checkNotNull(checkpointCoordinator);
				checkpointCoordinator.dropVertices(removedCandidates.toArray(new ExecutionVertex[0]), false);

				if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
					checkpointCoordinator.startCheckpointScheduler();
				}

				clean();

				// notify streamSwitch that change is finished
				streamSwitchAdaptor.onMigrationExecutorsStopped(targetVertex.getJobVertexId());
				streamSwitchAdaptor.onChangeImplemented(targetVertex.getJobVertexId());
			}, mainThreadExecutor);
	}

	private void failExecution(Throwable throwable) {
		LOG.info("++++++ Rescale failed with err: ", throwable);
		clean();
	}

	private void clean() {
		inProcess = false;
		notYetAcknowledgedTasks.clear();
	}


	@Override
	public void onReceiveRescalepointAcknowledge(ExecutionAttemptID attemptID, PendingCheckpoint checkpoint) {
		if (inProcess && checkpointId == checkpoint.getCheckpointId()) {

			CompletableFuture.runAsync(() -> {
				LOG.info("++++++ Received Rescalepoint Acknowledgement");
				try {
					synchronized (lock) {
						if (inProcess) {
							if (notYetAcknowledgedTasks.isEmpty()) {
								// late come in snapshot, ignore it
								return;
							}

							notYetAcknowledgedTasks.remove(attemptID);

							if (notYetAcknowledgedTasks.isEmpty()) {
								// receive all required snapshot, force streamSwitch to update metrices
//								streamSwitchAdaptor.onForceRetrieveMetrics(targetVertex.getJobVertexId());
								// only update executor mappings at this time.
//								streamSwitchAdaptor.onMigrationExecutorsStopped(targetVertex.getJobVertexId());

								LOG.info("++++++ handle operator states");
								handleCollectedStates(new HashMap<>(checkpoint.getOperatorStates()));
							}
						}
					}
				} catch (Exception e) {
					failExecution(e);
					throw new CompletionException(e);
				}
			}, mainThreadExecutor);
		}
	}

	public JobStatusListener createActivatorDeactivator() {
		if (jobStatusListener == null) {
			jobStatusListener = new JobRescaleCoordinatorDeActivator(this);
		}

		return jobStatusListener;
	}

	private static class JobRescaleCoordinatorDeActivator implements JobStatusListener {

		private final JobRescaleCoordinator coordinator;

		public JobRescaleCoordinatorDeActivator(JobRescaleCoordinator coordinator) {
			this.coordinator = checkNotNull(coordinator);
		}

		@Override
		public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error) {
			if (newJobStatus == JobStatus.RUNNING) {
				coordinator.start();
			} else {
				coordinator.stop();
			}
		}
	}
}
