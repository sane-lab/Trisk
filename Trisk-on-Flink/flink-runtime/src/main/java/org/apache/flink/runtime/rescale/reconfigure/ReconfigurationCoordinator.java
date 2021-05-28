package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.rescale.RescaleID;
import org.apache.flink.runtime.rescale.RescaleOptions;
import org.apache.flink.runtime.rescale.RescalepointAcknowledgeListener;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class ReconfigurationCoordinator extends AbstractCoordinator {

	private static final Logger LOG = LoggerFactory.getLogger(ReconfigurationCoordinator.class);
	private SynchronizeOperation currentSyncOp = null;

	public ReconfigurationCoordinator(JobGraph jobGraph, ExecutionGraph executionGraph) {
		super(jobGraph, executionGraph);
	}

	/**
	 * Synchronize the tasks in the list by inject barrier from source operator.
	 * When those tasks received barrier, they should stop processing and wait next instruction.
	 * <p>
	 * To pause processing, one solution is using the pause controller
	 * For non-source operator tasks, we use pause controller to stop reading message on the mailProcessor.
	 * <p>
	 * Regard as source stream tasks, since it does not have input, we will recreate its partitions with new configuration,
	 * thus its children stream task will temporarily not receive input from this source task until they update their input gates.
	 * Here we assume that the prepare api has been called before, so the execution graph already has the latest configuration.
	 * This is to prevent that down stream task received data that does not belongs to their key set.
	 * <p>
	 *
	 * @param taskList The list of task id, each id is a tuple which the first element is operator id and the second element is offset
	 * @return A future that representing  synchronize is successful
	 */
	@Override
	public CompletableFuture<Map<Integer, Map<Integer, AbstractCoordinator.Diff>>> synchronizeTasks(
		List<Tuple2<Integer, Integer>> taskList,
		@Nullable Map<Integer, Map<Integer, AbstractCoordinator.Diff>> diff) {
		// we should first check if the tasks is stateless
		// if stateless, do not need to synchronize,
		System.out.println("start synchronizing..." + taskList);
		LOG.info("++++++ start synchronizing..." + taskList);
		// stateful tasks, inject barrier
		SynchronizeOperation syncOp = new SynchronizeOperation(taskList);
		try {
			CompletableFuture<Map<OperatorID, OperatorState>> collectedOperatorStateFuture = syncOp.sync();
			// some check related here
			currentSyncOp = syncOp;
			return collectedOperatorStateFuture.thenApply(state ->
			{
				System.out.println("synchronizeTasks successful");
				LOG.debug("synchronizeTasks successful");
				// if update state is needed, try to re-assign state among those tasks
				checkNotNull(diff, "error while getting difference between old and new executionplan");
				for (Integer operatorID : diff.keySet()) {
					if (diff.get(operatorID).containsKey(KEY_STATE_ALLOCATION)) {
						checkNotNull(syncOp, "no state collected currently, have you synchronized first?");
						JobVertexID jobVertexID = rawVertexIDToJobVertexID(operatorID);
						ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexID);
						Preconditions.checkNotNull(executionJobVertex, "Execution job vertex not found: " + jobVertexID);
						OperatorWorkloadsAssignment remappingAssignment =
							(OperatorWorkloadsAssignment) diff.get(operatorID).get(AbstractCoordinator.KEY_STATE_ALLOCATION);
						StateAssignmentOperation stateAssignmentOperation =
							new StateAssignmentOperation(syncOp.checkpointId, Collections.singleton(executionJobVertex), state, true);
						stateAssignmentOperation.setForceRescale(true);
						stateAssignmentOperation.setRedistributeStrategy(remappingAssignment);
						LOG.info("++++++ start to assign states " + state);
						stateAssignmentOperation.assignStates();
						// can safely sync some old parameters because all modifications in JobMaster is completed.
						executionJobVertex.syncOldConfigInfo();
						LOG.info("++++++ assign states completed " + state);

					}
				}
				return diff;
			});
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	@Override
	public CompletableFuture<Map<Integer, Map<Integer, AbstractCoordinator.Diff>>> synchronizeTasks(
		Map<Integer, List<Integer>> tasks,
		@Nullable Map<Integer, Map<Integer, AbstractCoordinator.Diff>> diff) {
		// we should first check if the tasks is stateless
		// if stateless, do not need to synchronize,
		System.out.println("start synchronizing..." + tasks);
		LOG.info("++++++ start synchronizing..." + tasks);
		// stateful tasks, inject barrier
		SynchronizeOperation syncOp = new SynchronizeOperation(tasks);
		try {
			CompletableFuture<Map<OperatorID, OperatorState>> collectedOperatorStateFuture = syncOp.sync();
			// some check related here
			currentSyncOp = syncOp;
			return collectedOperatorStateFuture.thenApply(state ->
			{
				System.out.println("synchronizeTasks successful");
				LOG.debug("synchronizeTasks successful");
				// if update state is needed, try to re-assign state among those tasks
				checkNotNull(diff, "error while getting difference between old and new executionplan");
				for (Integer operatorID : diff.keySet()) {
					if (diff.get(operatorID).containsKey(KEY_STATE_ALLOCATION)) {
						checkNotNull(syncOp, "no state collected currently, have you synchronized first?");
						JobVertexID jobVertexID = rawVertexIDToJobVertexID(operatorID);
						ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexID);
						Preconditions.checkNotNull(executionJobVertex, "Execution job vertex not found: " + jobVertexID);
						OperatorWorkloadsAssignment remappingAssignment =
							(OperatorWorkloadsAssignment) diff.get(operatorID).get(AbstractCoordinator.KEY_STATE_ALLOCATION);
						StateAssignmentOperation stateAssignmentOperation =
							new StateAssignmentOperation(syncOp.checkpointId, Collections.singleton(executionJobVertex), state, true);
						stateAssignmentOperation.setForceRescale(true);
						stateAssignmentOperation.setRedistributeStrategy(remappingAssignment);
						LOG.info("++++++ start to assign states " + state);
						stateAssignmentOperation.assignStates();
						// can safely sync some old parameters because all modifications in JobMaster is completed.
						executionJobVertex.syncOldConfigInfo();
						LOG.info("++++++ assign states completed " + state);
					}
				}
				return diff;
			});
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	/**
	 * Resume the paused tasks by synchronize.
	 * <p>
	 * In implementation, since we use MailBoxProcessor to pause tasks,
	 * to make this resume methods make sense, the task's MailBoxProcessor should not be changed.
	 *
	 * @return
	 */
	@Override
	public CompletableFuture<Void> resumeTasks() {
		checkNotNull(currentSyncOp, "have you call sync op before?");
		return currentSyncOp.resumeAll().thenAccept(o -> currentSyncOp = null);
	}

	@Deprecated
	@Override
	public CompletableFuture<Void> updateTaskResources(int operatorID, int oldParallelism) {
		// TODO: By far we only support horizontal scaling, vertival scaling is not included.
//		System.out.println("++++++ re-allocate resources for tasks" + operatorID);
		JobVertexID tgtJobVertexID = rawVertexIDToJobVertexID(operatorID);
		ExecutionJobVertex tgtJobVertex = executionGraph.getJobVertex(tgtJobVertexID);
		assert tgtJobVertex != null;
		if (tgtJobVertex.getParallelism() < oldParallelism) {
			return cancelTasks(operatorID);
		} else if (tgtJobVertex.getParallelism() > oldParallelism) {
			return deployTasks(operatorID);
		} else {
			throw new IllegalStateException("none of new tasks has been created");
		}
	}

	@Override
	public CompletableFuture<Void> updateTaskResources(Map<Integer, List<Integer>> tasks, Map<Integer, List<SlotID>> slotIds) {
		// TODO: By far we only support horizontal scaling, vertival scaling is not included.
//		System.out.println("++++++ re-allocate resources for tasks" + operatorID);
		int operatorID = tasks.keySet().iterator().next();
		JobVertexID tgtJobVertexID = rawVertexIDToJobVertexID(operatorID);
		ExecutionJobVertex tgtJobVertex = executionGraph.getJobVertex(tgtJobVertexID);
//		assert tgtJobVertex != null;
//		if (isScaleIn) {
//			return cancelTasks(operatorID, 0);
//		} else {
//			return deployTasks(operatorID);
//		}
		CompletableFuture<Void> deployTaskFuture;
		if(slotIds == null){
			deployTaskFuture = deployTasks(operatorID);
		} else {
			// the parallelism parameter is useless
			List<SlotID> targetSlotIDs = slotIds.get(operatorID);
			deployTaskFuture = deployTasks(operatorID, 0, targetSlotIDs);
		}
		return deployTaskFuture
			.thenCompose(execution -> updateDownstreamGates(operatorID))
			.thenCompose(execution -> cancelTasks(operatorID));
	}

	public CompletableFuture<Void> deployTasks(int operatorID) {
		// TODO: add the task to the checkpointCoordinator
		System.out.println("deploying... tasks of " + operatorID);
		LOG.info("++++++ deploying tasks" + operatorID);
		JobVertexID jobVertexID = rawVertexIDToJobVertexID(operatorID);
		ExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);
		OperatorWorkloadsAssignment remappingAssignment = workloadsAssignmentHandler.getHeldOperatorWorkloadsAssignment(operatorID);
		Preconditions.checkNotNull(jobVertex, "Execution job vertex not found: " + jobVertexID);
		jobVertex.cleanBeforeRescale();

		Collection<CompletableFuture<Execution>> allocateSlotFutures =
			new ArrayList<>(jobVertex.getParallelism());

		if (!createdCandidates.containsKey(operatorID)) {
			return CompletableFuture.completedFuture(null);
		}

		for (ExecutionVertex vertex : createdCandidates.get(operatorID)) {
			Execution executionAttempt = vertex.getCurrentExecutionAttempt();
			allocateSlotFutures.add(executionAttempt.allocateAndAssignSlotForExecution(rescaleID));
		}

		return FutureUtils.combineAll(allocateSlotFutures)
			.whenComplete((executions, throwable) -> {
					if (throwable != null) {
						throwable.printStackTrace();
						throw new CompletionException(throwable);
					}
				}).thenCompose(executions -> {
					Collection<CompletableFuture<Void>> deployFutures =
						new ArrayList<>(jobVertex.getParallelism());
					for (Execution execution : executions) {
						try {
							deployFutures.add(execution.deploy(
								remappingAssignment.getAlignedKeyGroupRange(execution.getParallelSubtaskIndex()),
								remappingAssignment.getIdInModel(execution.getParallelSubtaskIndex())));
						} catch (JobException e) {
							e.printStackTrace();
						}
					}

					CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
					assert checkpointCoordinator != null;
					checkpointCoordinator.stopCheckpointScheduler();
					checkNotNull(checkpointCoordinator);
					checkpointCoordinator.addVertices(createdCandidates.get(operatorID).toArray(new ExecutionVertex[0]), false);

					if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
						checkpointCoordinator.startCheckpointScheduler();
					}

					// clear all created candidates
					createdCandidates.get(operatorID).clear();

					return FutureUtils.waitForAll(deployFutures);
				});
//		.thenCompose(executions -> {
//				CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
//				assert checkpointCoordinator != null;
//				checkpointCoordinator.stopCheckpointScheduler();
//				checkNotNull(checkpointCoordinator);
//				checkpointCoordinator.addVertices(createdCandidates.get(operatorID).toArray(new ExecutionVertex[0]), false);
//
//				if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
//					checkpointCoordinator.startCheckpointScheduler();
//				}
//
//				// clear all created candidates
//				createdCandidates.get(operatorID).clear();
//
//				// only deploy success that downstream task could start receiving data in its input gates
//				return updateDownstreamGates(operatorID);
//			});
	}

	private CompletableFuture<Void> deployTasks(int operatorID, int oldParallelism, List<SlotID> slotIds) {
		// TODO: add the task to the checkpointCoordinator
		System.out.println("deploying... tasks of " + operatorID);
		LOG.info("++++++ deploying tasks" + operatorID);
		JobVertexID jobVertexID = rawVertexIDToJobVertexID(operatorID);
		ExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);
		OperatorWorkloadsAssignment remappingAssignment = workloadsAssignmentHandler.getHeldOperatorWorkloadsAssignment(operatorID);
		Preconditions.checkNotNull(jobVertex, "Execution job vertex not found: " + jobVertexID);
		jobVertex.cleanBeforeRescale();

		Collection<CompletableFuture<Execution>> allocateSlotFutures =
			new ArrayList<>(jobVertex.getParallelism() - oldParallelism);

		if (slotIds != null) {
			List<ExecutionVertex> vertices = createdCandidates.get(operatorID);
			Preconditions.checkState(vertices.size() == slotIds.size(),  "number of given slot is not equal with createdCandidates");
			for (int i = 0; i < vertices.size(); i++) {
				Execution executionAttempt = vertices.get(i).getCurrentExecutionAttempt();
				LOG.info("++++++ allocating slots: " + slotIds.get(i));
				allocateSlotFutures.add(executionAttempt.allocateAndAssignSlotForExecution(rescaleID, slotIds.get(i)));
			}
		} else {
			for (ExecutionVertex vertex : createdCandidates.get(operatorID)) {
				Execution executionAttempt = vertex.getCurrentExecutionAttempt();
				allocateSlotFutures.add(executionAttempt.allocateAndAssignSlotForExecution(rescaleID));
			}
		}

		return FutureUtils.combineAll(allocateSlotFutures)
			.whenComplete((executions, throwable) -> {
					if (throwable != null) {
						throwable.printStackTrace();
						throw new CompletionException(throwable);
					}
				}).thenCompose(executions -> {
					Collection<CompletableFuture<Void>> deployFutures =
						new ArrayList<>(jobVertex.getParallelism() - oldParallelism);
					for (Execution execution : executions) {
						try {
							deployFutures.add(execution.deploy(
								remappingAssignment.getAlignedKeyGroupRange(execution.getParallelSubtaskIndex()),
								remappingAssignment.getIdInModel(execution.getParallelSubtaskIndex())));
						} catch (JobException e) {
							e.printStackTrace();
						}
					}

					CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
					assert checkpointCoordinator != null;
					checkpointCoordinator.stopCheckpointScheduler();
					checkNotNull(checkpointCoordinator);
					checkpointCoordinator.addVertices(createdCandidates.get(operatorID).toArray(new ExecutionVertex[0]), false);

					if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
						checkpointCoordinator.startCheckpointScheduler();
					}

					return FutureUtils.waitForAll(deployFutures);
				});
//			.thenCompose(executions -> {
//				CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
//				checkNotNull(checkpointCoordinator);
//				checkpointCoordinator.addVertices(createdCandidates.get(operatorID).toArray(new ExecutionVertex[0]), false);
//
//				// clear all created candidates
//				createdCandidates.get(operatorID).clear();
//
//				// only deploy success that downstream task could start receiving data in its input gates
//				return updateDownstreamGates(operatorID);
//			});
	}

	public CompletableFuture<Void> cancelTasks(int operatorID) {
		LOG.info("++++++ canceling tasks" + operatorID);

		if (!removedCandidates.containsKey(operatorID)) {
			return CompletableFuture.completedFuture(null);
		}

//		updateDownstreamGates(operatorID)
//			.thenCompose(executions -> {
				Collection<CompletableFuture<?>> removeFutures =
					new ArrayList<>(removedCandidates.size());

				for (ExecutionVertex vertex : removedCandidates.get(operatorID)) {
					CompletableFuture<?> removedTask = vertex.cancel();
					removeFutures.add(removedTask);
				}

				CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
				assert checkpointCoordinator != null;
				checkpointCoordinator.stopCheckpointScheduler();
				checkNotNull(checkpointCoordinator);
				checkpointCoordinator.dropVertices(removedCandidates.get(operatorID).toArray(new ExecutionVertex[0]), false);

				if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
					checkpointCoordinator.startCheckpointScheduler();
				}

				// clear all removed candidates
				removedCandidates.get(operatorID).clear();

				return FutureUtils.waitForAll(removeFutures);
//			});
//		return CompletableFuture.completedFuture(null);
	}

	/**
	 * update result partition on this operator,
	 * update input gates, key group range on its down stream operator
	 *
	 * @return
	 */
	@Override
	public CompletableFuture<Map<Integer, Map<Integer, Diff>>> updateKeyMapping(
		Map<Integer, List<Integer>> tasks,
		@Nonnull Map<Integer, Map<Integer, Diff>> diff) {
		System.out.println("update mapping...");
		LOG.info("++++++ update Key Mapping");

		int targetOperatorID = tasks.keySet().iterator().next();
		// update key group range in target stream
		JobVertexID targetJobVertexID = rawVertexIDToJobVertexID(targetOperatorID);
		ExecutionJobVertex targetJobVertex = executionGraph.getJobVertex(targetJobVertexID);
		checkNotNull(targetJobVertex, "Execution job vertex not found: " + targetJobVertexID);
		final SynchronizeOperation syncOp = this.currentSyncOp;

		final List<CompletableFuture<Void>> rescaleCandidatesFutures = new ArrayList<>();
		try {
			for (OperatorDescriptor upstreamOperator : heldExecutionPlan.getOperatorByID(targetOperatorID).getParents()) {
				// todo some partitions may not need modified, for example, broad cast partitioner
				rescaleCandidatesFutures
					.add(
						updatePartitions(upstreamOperator.getOperatorID(), rescaleID)
							.thenCompose(o -> updateDownstreamGates(upstreamOperator.getOperatorID()))
//							.thenCompose(o -> {
//								// check should we resume those tasks
//								Map<Integer, Diff> diffMap = diff.get(upstreamOperator.getOperatorID());
//								diffMap.remove(AbstractCoordinator.KEY_MAPPING);
//								if (diffMap.isEmpty() && syncOp != null) {
//									return syncOp.resumeTasks(Collections.singletonList(Tuple2.of(upstreamOperator.getOperatorID(), -1)));
//								}
//								return CompletableFuture.completedFuture(null);
//							})
				);
				// check should we resume those tasks
				Map<Integer, Diff> diffMap = diff.get(upstreamOperator.getOperatorID());
				diffMap.remove(AbstractCoordinator.KEY_MAPPING);
				if (diffMap.isEmpty() && syncOp != null) {
					syncOp.resumeTasks(Collections.singletonList(Tuple2.of(upstreamOperator.getOperatorID(), -1)));
				}
			}

			OperatorWorkloadsAssignment remappingAssignment = workloadsAssignmentHandler.getHeldOperatorWorkloadsAssignment(targetOperatorID);
			List<Tuple2<Integer, Integer>> notModifiedList = new ArrayList<>();
			for (int subtaskIndex = 0; subtaskIndex < targetJobVertex.getParallelism(); subtaskIndex++) {
				ExecutionVertex vertex = targetJobVertex.getTaskVertices()[subtaskIndex];
				Execution execution = vertex.getCurrentExecutionAttempt();
				if (execution != null && execution.getState() == ExecutionState.RUNNING) {
					rescaleCandidatesFutures.add(execution.scheduleRescale(
						rescaleID,
						RescaleOptions.RESCALE_KEYGROUP_RANGE_ONLY,
						remappingAssignment.getAlignedKeyGroupRange(subtaskIndex)));
					// if is not a modified task and do not need to update partitions, then resume.
					if (!remappingAssignment.isTaskModified(subtaskIndex)) {
						notModifiedList.add(Tuple2.of(targetOperatorID, subtaskIndex));
					}
				} else {
					vertex.assignKeyGroupRange(remappingAssignment.getAlignedKeyGroupRange(subtaskIndex));
				}
			}
			// TODO: before resume those tasks, should check whether the update on the task has been completed, using diffmap
			// TODO: but diffmap is operator-centric, and is unable to check whether the task should be resumed.
			if (remappingAssignment.isScaling()) {
				try { // update partition and downstream gates if there are tasks to be scaled out/in
					updatePartitions(targetOperatorID, rescaleID)
						// downstream gates need to find out the upstream partitions, do not update downstream gates before upstream has updated
//						.thenCompose(o -> updateDownstreamGates(targetOperatorID))
						.thenAccept(o -> syncOp.resumeTasks(notModifiedList));
				} catch (ExecutionGraphException e) {
					e.printStackTrace();
				}
			} else {
				syncOp.resumeTasks(notModifiedList);
			}

			CompletableFuture<Void> finishFuture = FutureUtils.completeAll(rescaleCandidatesFutures);
//			finishFuture.thenAccept(
////			return finishFuture.thenApply(
//				o -> {
//					for (OperatorDescriptor upstreamOperator : heldExecutionPlan.getOperatorDescriptorByID(targetOperatorID).getParents()) {
//						// check should we resume those tasks
//						Map<Integer, Diff> diffMap = diff.get(upstreamOperator.getOperatorID());
//						diffMap.remove(AbstractCoordinator.KEY_MAPPING);
//						if (diffMap.isEmpty() && syncOp != null) {
//							syncOp.resumeTasks(Collections.singletonList(Tuple2.of(upstreamOperator.getOperatorID(), -1)));
//						}
//					}
////					return diff;
//				}
//			);
			// the tasks can be resumed asynchrouously
			return finishFuture.thenApply(o -> {
				LOG.info("++++++ completed key mapping");
				return diff;
			});
		} catch (ExecutionGraphException e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	/**
	 * update result partition on this operator,
	 * update input gates, key group range on its down stream operator
	 *
	 * @return
	 */
	@Override
	public CompletableFuture<Map<Integer, Map<Integer, Diff>>> updateKeyMapping(
		int targetOperatorID,
//		Map<Integer, List<Integer>> tasks,
		@Nonnull Map<Integer, Map<Integer, Diff>> diff) {

		System.out.println("update mapping...");
		LOG.info("++++++ update Key Mapping");

		final List<CompletableFuture<Void>> rescaleCandidatesFutures = new ArrayList<>();
		try {
			for (OperatorDescriptor upstreamOperator : heldExecutionPlan.getOperatorByID(targetOperatorID).getParents()) {
				// todo some partitions may not need modified, for example, broad cast partitioner
				rescaleCandidatesFutures
					.add(updatePartitions(upstreamOperator.getOperatorID(), rescaleID).thenCompose(o -> updateDownstreamGates(upstreamOperator.getOperatorID()))
					);
			}
			// update key group range in target stream
			JobVertexID targetJobVertexID = rawVertexIDToJobVertexID(targetOperatorID);
			ExecutionJobVertex targetJobVertex = executionGraph.getJobVertex(targetJobVertexID);
			checkNotNull(targetJobVertex, "Execution job vertex not found: " + targetJobVertexID);
//			RemappingAssignment remappingAssignment = new RemappingAssignment(
//				heldExecutionPlan.getKeyStateAllocation(destOpID)
//			);

			OperatorWorkloadsAssignment remappingAssignment = workloadsAssignmentHandler.getHeldOperatorWorkloadsAssignment(targetOperatorID);
			for (int subtaskIndex = 0; subtaskIndex < targetJobVertex.getParallelism(); subtaskIndex++) {
				ExecutionVertex vertex = targetJobVertex.getTaskVertices()[subtaskIndex];
				Execution execution = vertex.getCurrentExecutionAttempt();
				if (execution != null && execution.getState() == ExecutionState.RUNNING) {
					rescaleCandidatesFutures.add(execution.scheduleRescale(
						rescaleID,
						RescaleOptions.RESCALE_KEYGROUP_RANGE_ONLY,
						remappingAssignment.getAlignedKeyGroupRange(subtaskIndex)));
				} else {
					vertex.assignKeyGroupRange(remappingAssignment.getAlignedKeyGroupRange(subtaskIndex));
				}
			}
			final SynchronizeOperation syncOp = this.currentSyncOp;
			CompletableFuture<Void> finishFuture = FutureUtils.completeAll(rescaleCandidatesFutures);
			finishFuture.thenAccept(
				o -> {
					for (OperatorDescriptor targetOperator : heldExecutionPlan.getOperatorByID(targetOperatorID).getParents()) {
						// check should we resume those tasks
						Map<Integer, Diff> diffMap = diff.get(targetOperator.getOperatorID());
						diffMap.remove(AbstractCoordinator.KEY_MAPPING);
						if (diffMap.isEmpty() && syncOp != null) {
							syncOp.resumeTasks(Collections.singletonList(Tuple2.of(targetOperator.getOperatorID(), -1)));
						}
					}
				}
			);
			return finishFuture.thenApply(o -> diff);
		} catch (ExecutionGraphException e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	private CompletableFuture<Void> updatePartitions(int operatorID, RescaleID rescaleID) throws ExecutionGraphException {
		// update result partition
		JobVertexID jobVertexID = rawVertexIDToJobVertexID(operatorID);
		ExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);
		Preconditions.checkNotNull(jobVertex, "Execution job vertex not found: " + jobVertexID);
		List<CompletableFuture<Void>> updatePartitionsFuture = new ArrayList<>();
		if (!jobVertex.getInputs().isEmpty() || currentSyncOp == null) {
			// the source operator has updated its partitions during synchronization, skip source operator partition update
			jobVertex.cleanBeforeRescale();
			for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
				Execution execution = vertex.getCurrentExecutionAttempt();
				if (!vertex.getRescaleId().equals(rescaleID) && execution != null && execution.getState() == ExecutionState.RUNNING) {
					execution.updateProducedPartitions(rescaleID);
					updatePartitionsFuture.add(execution.scheduleRescale(rescaleID, RescaleOptions.RESCALE_PARTITIONS_ONLY, null));
				}
			}
		}
		return FutureUtils.completeAll(updatePartitionsFuture);
	}

	private CompletableFuture<Void> updateDownstreamGates(int operatorID) {
		List<CompletableFuture<Void>> updateGatesFuture = new ArrayList<>();
		// update input gates in child stream of source op
		for (OperatorDescriptor downstreamOperator : heldExecutionPlan.getOperatorByID(operatorID).getChildren()) {
			try {
				updateGates(downstreamOperator.getOperatorID(), updateGatesFuture);
			} catch (ExecutionGraphException e) {
				e.printStackTrace();
			}
		}
		return FutureUtils.completeAll(updateGatesFuture);
	}

	private void updateGates(int operatorID, List<CompletableFuture<Void>> futureList) throws ExecutionGraphException {
		JobVertexID jobVertexID = rawVertexIDToJobVertexID(operatorID);
		ExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);
		Preconditions.checkNotNull(jobVertex, "Execution job vertex not found: " + jobVertexID);
		for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
			Execution execution = vertex.getCurrentExecutionAttempt();
			if (execution != null && execution.getState() == ExecutionState.RUNNING) {
				futureList.add(execution.scheduleRescale(rescaleID, RescaleOptions.RESCALE_GATES_ONLY, null));
			}
		}
	}

	/**
	 * update the key state in destination operator
	 *
	 //	 * @param operatorID the id of operator that need to update state
	 	 * @param tasks the affected tasks that need to update state
	 * @return
	 */
	@Override
	public CompletableFuture<Map<Integer, Map<Integer, AbstractCoordinator.Diff>>> updateState(
		Map<Integer, List<Integer>> tasks,
		Map<Integer, Map<Integer, AbstractCoordinator.Diff>> diff) {
		System.out.println("update state...");
		LOG.info("++++++ update State");
		checkNotNull(currentSyncOp, "have you call sync before");
		final SynchronizeOperation syncOp = this.currentSyncOp;

		int operatorID = tasks.keySet().iterator().next();

		JobVertexID jobVertexID = rawVertexIDToJobVertexID(operatorID);

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexID);
		Preconditions.checkNotNull(executionJobVertex, "Execution job vertex not found: " + jobVertexID);

		Map<Integer, Diff> diffMap = diff.get(operatorID);
		OperatorWorkloadsAssignment remappingAssignment = (OperatorWorkloadsAssignment) diffMap.remove(AbstractCoordinator.KEY_STATE_ALLOCATION);

//		CompletableFuture<Void> updateTargetPartitionFuture = CompletableFuture.completedFuture(null);

		CompletableFuture<Void> assignStateFuture = CompletableFuture.completedFuture(null);
//		final CompletableFuture<Void> finalUpdateTargetPartitionFuture = updateTargetPartitionFuture;
		return assignStateFuture.thenCompose(o -> {
				final List<CompletableFuture<?>> rescaleCandidatesFutures = new ArrayList<>();
				for (int subtaskIndex = 0; subtaskIndex < executionJobVertex.getParallelism(); subtaskIndex++) {
					Execution execution = executionJobVertex.getTaskVertices()[subtaskIndex].getCurrentExecutionAttempt();
					// for those unmodified tasks, update keygroup range, for those modified tasks, update state.
					if (!remappingAssignment.isTaskModified(subtaskIndex)) {
						continue;
					}
					if (execution != null && execution.getState() == ExecutionState.RUNNING) {
						try {
							System.out.println(operatorID + " update state at: " + subtaskIndex + " id in model: " + remappingAssignment.getIdInModel(subtaskIndex));
							CompletableFuture<Void> stateUpdateFuture = execution.scheduleRescale(
								rescaleID,
								RescaleOptions.RESCALE_STATE_ONLY,
								remappingAssignment.getAlignedKeyGroupRange(subtaskIndex),
								remappingAssignment.getIdInModel(subtaskIndex)
							);
							if (diffMap.isEmpty()) {
								checkNotNull(syncOp, "have you call sync before?");
								final int taskId = subtaskIndex;
								stateUpdateFuture.thenRun(
									() -> syncOp.resumeTasks(Collections.singletonList(Tuple2.of(operatorID, taskId)))
								);
							}
							rescaleCandidatesFutures.add(stateUpdateFuture);
						} catch (ExecutionGraphException e) {
							e.printStackTrace();
						}
					}
				}
				return FutureUtils.completeAll(rescaleCandidatesFutures);
			}
		).thenApply(o -> {
			LOG.info("++++++ update state completed");
			return diff;
		});
	}

	/**
	 * update the key state in destination operator
	 *
	 * @param operatorID the id of operator that need to update state
	 * @return
	 */
	@Override
	public CompletableFuture<Map<Integer, Map<Integer, AbstractCoordinator.Diff>>> updateState(
		int operatorID,
		Map<Integer, Map<Integer, AbstractCoordinator.Diff>> diff) {
		System.out.println("update state...");
		LOG.info("++++++ update State");
		checkNotNull(currentSyncOp, "have you call sync before");
		final SynchronizeOperation syncOp = this.currentSyncOp;

		JobVertexID jobVertexID = rawVertexIDToJobVertexID(operatorID);

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexID);
		Preconditions.checkNotNull(executionJobVertex, "Execution job vertex not found: " + jobVertexID);

		Map<Integer, Diff> diffMap = diff.get(operatorID);
		OperatorWorkloadsAssignment remappingAssignment = (OperatorWorkloadsAssignment) diffMap.remove(AbstractCoordinator.KEY_STATE_ALLOCATION);

		CompletableFuture<Void> updateTargetPartitionFuture = CompletableFuture.completedFuture(null);
		if (diffMap.isEmpty()) { // TODO: this is weird, why need to add a diffMap is empty?
			// means the state set allocation do not have change, update state should finish
			if (remappingAssignment == null) {
				syncOp.resumeTasks(Collections.singletonList(Tuple2.of(operatorID, -1)));
				return CompletableFuture.completedFuture(diff);
			} else {
				List<Tuple2<Integer, Integer>> notModifiedList =
					Arrays.stream(executionJobVertex.getTaskVertices())
						.map(ExecutionVertex::getParallelSubtaskIndex)
						.filter(i -> !remappingAssignment.isTaskModified(i))
						.map(i -> Tuple2.of(operatorID, i))
						.collect(Collectors.toList());
				if (remappingAssignment.isScaling()) {
					try {
						updateTargetPartitionFuture = updatePartitions(operatorID, rescaleID);
						updateTargetPartitionFuture.thenAccept(o -> syncOp.resumeTasks(notModifiedList));
					} catch (ExecutionGraphException e) {
						e.printStackTrace();
					}
				} else {
					syncOp.resumeTasks(notModifiedList);
				}
			}
		}

		checkNotNull(syncOp, "no state collected currently, have you synchronized first?");
		CompletableFuture<Void> assignStateFuture = syncOp.finishedFuture.thenAccept(
			state -> {
				StateAssignmentOperation stateAssignmentOperation =
					new StateAssignmentOperation(syncOp.checkpointId, Collections.singleton(executionJobVertex), state, true);
				stateAssignmentOperation.setForceRescale(true);
				// think about latter
				stateAssignmentOperation.setRedistributeStrategy(remappingAssignment);

				LOG.info("++++++ start to assign states " + state);
				stateAssignmentOperation.assignStates();
				// can safely sync the some old parameters because all modifications in JobMaster is completed.
				executionJobVertex.syncOldConfigInfo();
			}
		);
		final CompletableFuture<Void> finalUpdateTargetPartitionFuture = updateTargetPartitionFuture;
		return assignStateFuture.thenCompose(o -> {
				final List<CompletableFuture<?>> rescaleCandidatesFutures = new ArrayList<>();
				for (int subtaskIndex = 0; subtaskIndex < executionJobVertex.getParallelism(); subtaskIndex++) {
					Execution execution = executionJobVertex.getTaskVertices()[subtaskIndex].getCurrentExecutionAttempt();
					// for those unmodified tasks, update keygroup range, for those modified tasks, update state.
					if (!remappingAssignment.isTaskModified(subtaskIndex)) {
						continue;
					}
					if (execution != null && execution.getState() == ExecutionState.RUNNING) {
						try {
							System.out.println(operatorID + " update state at: " + subtaskIndex + " id in model: " + remappingAssignment.getIdInModel(subtaskIndex));
							CompletableFuture<Void> stateUpdateFuture = execution.scheduleRescale(
								rescaleID,
								RescaleOptions.RESCALE_STATE_ONLY,
								remappingAssignment.getAlignedKeyGroupRange(subtaskIndex),
								remappingAssignment.getIdInModel(subtaskIndex)
							);
							rescaleCandidatesFutures.add(stateUpdateFuture);
							if (diffMap.isEmpty()) {
								checkNotNull(syncOp, "have you call sync before?");
								final int taskOffset = subtaskIndex;
								stateUpdateFuture.runAfterBoth(
									finalUpdateTargetPartitionFuture,
									() -> syncOp.resumeTasks(Collections.singletonList(Tuple2.of(operatorID, taskOffset)))
								);
							}
						} catch (ExecutionGraphException e) {
							e.printStackTrace();
						}
					}
				}
				return FutureUtils.completeAll(rescaleCandidatesFutures);
			}
		).thenApply(o -> diff);
	}

	/**
	 * This method also will wake up the paused tasks.
	 *
	 * @param vertexID the operator id of this operator
	 * @return
	 */
	@Override
	public CompletableFuture<Map<Integer, Map<Integer, AbstractCoordinator.Diff>>> updateFunction(
		int vertexID, Map<Integer,
		Map<Integer, AbstractCoordinator.Diff>> diff) {

		System.out.println("some one want to triggerOperatorUpdate?");
		OperatorID operatorID = super.operatorIDMap.get(vertexID);
		JobVertexID jobVertexID = rawVertexIDToJobVertexID(vertexID);

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexID);
		Preconditions.checkNotNull(executionJobVertex, "Execution job vertex not found: " + jobVertexID);

		final List<CompletableFuture<?>> resultFutures =
			Arrays.stream(executionJobVertex.getTaskVertices())
				.map(ExecutionVertex::getCurrentExecutionAttempt)
				.filter(execution -> execution != null && execution.getState() == ExecutionState.RUNNING)
				.map(execution -> {
						try {
							return execution.scheduleOperatorUpdate(operatorID);
						} catch (Exception e) {
							e.printStackTrace();
							return FutureUtils.completedExceptionally(e);
						}
					}
				).collect(Collectors.toList());

		Map<Integer, Diff> diffMap = diff.get(vertexID);
		diffMap.remove(AbstractCoordinator.UDF);
		final SynchronizeOperation syncOp = this.currentSyncOp;
		final boolean resume = diffMap.isEmpty() && syncOp != null;

		CompletableFuture<Void> finishFuture = FutureUtils.completeAll(resultFutures);
		finishFuture.thenAccept(o -> {
			if (resume) {
				syncOp.resumeTasks(Collections.singletonList(Tuple2.of(vertexID, -1)));
			}
		});
		return finishFuture.thenApply(o -> diff);
	}

	/**
	 * This method also will wake up the paused tasks.
	 *
	 * @param tasks the tasks to update function
	 * @return
	 */
	@Override
	public CompletableFuture<Map<Integer, Map<Integer, AbstractCoordinator.Diff>>> updateFunction(
		Map<Integer, List<Integer>> tasks,
		Map<Integer, Map<Integer, AbstractCoordinator.Diff>> diff) {

		System.out.println("some one want to triggerOperatorUpdate?");

		int vertexID = tasks.keySet().iterator().next();

		OperatorID operatorID = super.operatorIDMap.get(vertexID);
		JobVertexID jobVertexID = rawVertexIDToJobVertexID(vertexID);

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexID);
		Preconditions.checkNotNull(executionJobVertex, "Execution job vertex not found: " + jobVertexID);

		final List<CompletableFuture<?>> resultFutures =
			Arrays.stream(executionJobVertex.getTaskVertices())
				.map(ExecutionVertex::getCurrentExecutionAttempt)
				.filter(execution -> execution != null && execution.getState() == ExecutionState.RUNNING)
				.map(execution -> {
						try {
							return execution.scheduleOperatorUpdate(operatorID);
						} catch (Exception e) {
							e.printStackTrace();
							return FutureUtils.completedExceptionally(e);
						}
					}
				).collect(Collectors.toList());

		Map<Integer, Diff> diffMap = diff.get(vertexID);
		diffMap.remove(AbstractCoordinator.UDF);
		final SynchronizeOperation syncOp = this.currentSyncOp;
		final boolean resume = diffMap.isEmpty() && syncOp != null;

		CompletableFuture<Void> finishFuture = FutureUtils.completeAll(resultFutures);
		finishFuture.thenAccept(o -> {
			if (resume) {
				syncOp.resumeTasks(Collections.singletonList(Tuple2.of(vertexID, -1)));
			}
		});
		return finishFuture.thenApply(o -> diff);
	}

	/**
	 * get actual affected execution vertices in Flink
	 * @param ejv
	 */
	private List<ExecutionVertex> getAfftectedVertices(ExecutionJobVertex ejv) {
		return new ArrayList<>(Arrays.asList(ejv.getTaskVertices()));
	}

	// to convert the task [(tid, -1), ] to [(tid, 0), (tid, 1), (tid, 2), ...]
	private List<Tuple2<Integer, Integer>> convertToNoneNegativeOffsetList(List<Tuple2<Integer, Integer>> vertexIDList) {
		List<Tuple2<Integer, Integer>> convertedVertexIDList = new ArrayList<>(vertexIDList.size());
		for (Tuple2<Integer, Integer> vertexID : vertexIDList) {
			ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(rawVertexIDToJobVertexID(vertexID.f0));
			checkNotNull(executionJobVertex, "can not find the job vertex" + vertexID.toString());
			if (vertexID.f1 < 0) {
				convertedVertexIDList.addAll(
					Arrays.stream(executionJobVertex.getTaskVertices())
						.filter(e -> e.getCurrentExecutionAttempt() != null &&
							(e.getCurrentExecutionAttempt().getState() == ExecutionState.RUNNING)
							|| e.getCurrentExecutionAttempt().getState() == ExecutionState.DEPLOYING)
						.map(e -> Tuple2.of(vertexID.f0, e.getParallelSubtaskIndex()))
						.collect(Collectors.toList())
				);
			} else {
//				checkArgument(vertexID.f1 < executionJobVertex.getParallelism(), "offset out of boundary");
				boolean isValid = false;
				for (ExecutionVertex vertex : executionJobVertex.getTaskVertices()) {
					if (vertexID.f1 == vertex.getParallelSubtaskIndex()) {
						convertedVertexIDList.add(vertexID);
						isValid = true;
						break;
					}
				}
				// if the task is to be removed, it is a valid task but is not a task to be resumed in the future
				// we can safely remove the task from the pausedtask, because we will update pause all removed tasks next.
				if (!isValid) {
					for (ExecutionVertex vertex : removedCandidates.get(vertexID.f0)) {
						if (vertexID.f1 == vertex.getParallelSubtaskIndex()) {
							isValid = true;
							break;
						}
					}
				}
				checkArgument(isValid, "subtaskindex is not found in executionvertex", vertexID);
			}
		}
		return convertedVertexIDList;
	}

	// temporary use RescalepointAcknowledgeListener
	private class SynchronizeOperation implements RescalepointAcknowledgeListener {

		private final Set<ExecutionAttemptID> notYetAcknowledgedTasks = new HashSet<>();

		private final List<JobVertexID> jobVertexIdList;
		private final List<Tuple2<Integer, Integer>> pausedTasks;
		private final Object lock = new Object();

		private final CompletableFuture<Map<OperatorID, OperatorState>> finishedFuture;

		private long checkpointId;

		SynchronizeOperation(List<Tuple2<Integer, Integer>> taskList) {
			this.jobVertexIdList = taskList.stream()
				.map(t -> rawVertexIDToJobVertexID(t.f0))
				.collect(Collectors.toList());
			this.pausedTasks = convertToNoneNegativeOffsetList(taskList);
			finishedFuture = new CompletableFuture<>();
		}

		SynchronizeOperation(Map<Integer, List<Integer>> tasks) {
			List<Tuple2<Integer, Integer>> taskList = tasks.keySet().stream()
				.map(t -> Tuple2.of(t, -1))
				.collect(Collectors.toList());
//			for (int rawId : tasks.keySet()) {
//				taskList.addAll(tasks.get(rawId).stream()
//					.map(t -> Tuple2.of(rawId, t))
//					.collect(Collectors.toList()));
//			}
			this.jobVertexIdList = taskList.stream()
				.map(t -> rawVertexIDToJobVertexID(t.f0))
				.collect(Collectors.toList());

			this.pausedTasks = convertToNoneNegativeOffsetList(taskList);
			finishedFuture = new CompletableFuture<>();
		}

		private CompletableFuture<Map<OperatorID, OperatorState>> sync() throws ExecutionGraphException {
			// add needed acknowledge tasks
			List<CompletableFuture<Void>> affectedExecutionPrepareSyncFutures = new LinkedList<>();
			for (JobVertexID jobVertexId : jobVertexIdList) {
				ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexId);
				checkNotNull(executionJobVertex, "can not find the job vertex" + jobVertexId);
				List<ExecutionVertex> affectededVertices = getAfftectedVertices(executionJobVertex);
				if (executionJobVertex.getInputs().isEmpty()) {
					// this is source task vertex
					affectedExecutionPrepareSyncFutures.add(pauseSourceStreamTask(executionJobVertex, affectededVertices));
				} else {
					// sync affected existing task vertices
					affectededVertices.stream()
						.map(ExecutionVertex::getCurrentExecutionAttempt)
						.filter(execution -> execution != null &&
							(execution.getState() == ExecutionState.RUNNING || execution.getState() == ExecutionState.DEPLOYING))
						.forEach(execution -> {
								affectedExecutionPrepareSyncFutures.add(execution.scheduleForInterTaskSync(TaskOperatorManager.NEED_SYNC_REQUEST));
								notYetAcknowledgedTasks.add(execution.getAttemptId());
							}
						);
				}
			}

			// sync tasks to be removed in the new configuration
			// becuase it was removed from executionJobVertex, need to use the removed candidates to store them
			for (Map.Entry<Integer, List<ExecutionVertex>> entry : removedCandidates.entrySet()) {
				for (ExecutionVertex vertex :  entry.getValue()) {
					Execution execution = vertex.getCurrentExecutionAttempt();
					affectedExecutionPrepareSyncFutures.add(execution.scheduleForInterTaskSync(TaskOperatorManager.NEED_SYNC_REQUEST));
					notYetAcknowledgedTasks.add(execution.getAttemptId());
				}
			}

			// make affected tasks prepare synchronization
			FutureUtils.completeAll(affectedExecutionPrepareSyncFutures)
				.thenRunAsync(() -> {
					try {
						CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
						checkNotNull(checkpointCoordinator, "do not have checkpointCoordinator");
						checkpointCoordinator.stopCheckpointScheduler();
						checkpointCoordinator.setRescalepointAcknowledgeListener(this);
						// temporary use rescale point
						System.out.println("send barrier...");
						checkpointCoordinator.triggerRescalePoint(System.currentTimeMillis());
					} catch (Exception e) {
						throw new CompletionException(e);
					}
				});
			return finishedFuture;
		}

		private CompletableFuture<Void> pauseSourceStreamTask(
			ExecutionJobVertex executionJobVertex,
			List<ExecutionVertex> affectedVertices) throws ExecutionGraphException {

			List<CompletableFuture<Void>> futureList = new ArrayList<>();
			executionJobVertex.cleanBeforeRescale();
			for (ExecutionVertex executionVertex : affectedVertices) {
				Execution execution = executionVertex.getCurrentExecutionAttempt();
				if (execution != null && execution.getState() == ExecutionState.RUNNING) {
					execution.updateProducedPartitions(rescaleID);
					futureList.add(execution.scheduleRescale(rescaleID, RescaleOptions.PREPARE_ONLY, null));
					notYetAcknowledgedTasks.add(execution.getAttemptId());
				}
			}
			return FutureUtils.completeAll(futureList);
		}

		private CompletableFuture<Void> resumeAll() {
			return resumeTasks(pausedTasks);
		}

		private CompletableFuture<Void> resumeTasks(List<Tuple2<Integer, Integer>> taskList) {
			System.out.println("resuming..." + taskList);
			taskList = convertToNoneNegativeOffsetList(taskList);
			List<CompletableFuture<Void>> affectedExecutionPrepareSyncFutures = new LinkedList<>();
			boolean isSourceResumed = false;
			for (Tuple2<Integer, Integer> taskID : taskList) {
				synchronized (pausedTasks) {
					if (!this.pausedTasks.remove(taskID)) {
						continue;
					}
				}
				JobVertexID jobVertexID = rawVertexIDToJobVertexID(taskID.f0);
				ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexID);
				checkNotNull(executionJobVertex);
				ExecutionVertex operatedVertex = executionJobVertex.getTaskVertices()[taskID.f1];

				if (executionJobVertex.getInputs().isEmpty()) {
					// To resume source task, we need to update its down stream gates,
					// in some case, this may have been done during update mapping.
					//
					// Since we could ensure that we will only update those partition id changed
					// gates, thus in this case, it is ok cause resumeSourceStreamTask will do nothing
					if (!isSourceResumed) { // only need to update once for downstream tasks
						affectedExecutionPrepareSyncFutures.add(updateDownstreamGates(taskID.f0));
					}
					isSourceResumed = true;
				} else {
					Execution execution = operatedVertex.getCurrentExecutionAttempt();
					affectedExecutionPrepareSyncFutures.add(execution.scheduleForInterTaskSync(TaskOperatorManager.NEED_RESUME_REQUEST));
				}
			}
			// make affected task resume
			return FutureUtils.completeAll(affectedExecutionPrepareSyncFutures);
		}

		@Override
		public void onReceiveRescalepointAcknowledge(ExecutionAttemptID attemptID, PendingCheckpoint checkpoint) {
			if (checkpointId == checkpoint.getCheckpointId()) {
				CompletableFuture.runAsync(() -> {
					LOG.info("++++++ Received Rescalepoint Acknowledgement:" + attemptID);
					try {
						synchronized (lock) {
							if (notYetAcknowledgedTasks.isEmpty()) {
								// late come in snapshot, ignore it
								return;
							}
							notYetAcknowledgedTasks.remove(attemptID);

							if (notYetAcknowledgedTasks.isEmpty()) {
								LOG.info("++++++ handle operator states");
								CheckpointCoordinator checkpointCoordinator = executionGraph.getCheckpointCoordinator();
								checkNotNull(checkpointCoordinator);
								if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
									LOG.info("++++++ resume checkpoint coordinator");
									checkpointCoordinator.startCheckpointScheduler();
								}
								LOG.info("++++++ received operator states" + checkpoint.getOperatorStates() + " : " + finishedFuture);
								finishedFuture.complete(new HashMap<>(checkpoint.getOperatorStates()));
							}
						}
					} catch (Exception e) {
						throw new CompletionException(e);
					}
				});
			}
		}

		@Override
		public void setCheckpointId(long checkpointId) {
			this.checkpointId = checkpointId;
			System.out.println("trigger rescale point with check point id:" + checkpointId);
		}
	}

}
