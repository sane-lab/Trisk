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

package org.apache.flink.streaming.controlplane.streammanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.ExecutionPlanAndJobGraphUpdaterFactory;
import org.apache.flink.runtime.controlplane.PrimitiveOperation;
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.resource.AbstractSlot;
import org.apache.flink.runtime.controlplane.abstraction.resource.FlinkSlot;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerGateway;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerId;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.rescale.JobRescaleAction;
import org.apache.flink.runtime.rescale.reconfigure.AbstractCoordinator;
import org.apache.flink.runtime.rescale.reconfigure.JobGraphRescaler;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdActions;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.registration.JobManagerRegistration;
import org.apache.flink.runtime.resourcemanager.slotmanager.TaskManagerSlot;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.util.profiling.ReconfigurationProfiler;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.streaming.controlplane.jobgraph.DefaultExecutionPlanAndJobGraphUpdaterFactory;
import org.apache.flink.streaming.controlplane.rescale.StreamJobGraphRescaler;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ExecutionPlanWithLock;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.apache.flink.streaming.controlplane.streammanager.exceptions.StreamManagerException;
import org.apache.flink.streaming.controlplane.udm.*;
import org.apache.flink.util.OptionalConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor.ExecutionLogic.UDF;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * @author trx
 * StreamManager implementation.
 * <p>
 * TODO:
 * 1. decide other fields
 * 2. initialize other fields
 * 3. i do not know how to decouple the connection between stream manager and flink job master
 */
public class StreamManager extends FencedRpcEndpoint<StreamManagerId> implements StreamManagerGateway, StreamManagerService, ReconfigurationExecutor {


	private static final Logger LOG = LoggerFactory.getLogger(StreamManager.class);
	/**
	 * Default names for Flink's distributed components.
	 */
	public static final String Stream_Manager_NAME = "streammanager";

	private final StreamManagerConfiguration streamManagerConfiguration;

	private final ResourceID resourceId;

	private final JobGraph jobGraph;

	private final ClassLoader userCodeLoader;

	private final Time rpcTimeout;

	private final HighAvailabilityServices highAvailabilityServices;

	private final FatalErrorHandler fatalErrorHandler;

	private final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever;

	@Deprecated
	private final JobGraphRescaler jobGraphRescaler;

	private final Map<String, ControlPolicy> controlPolicyList = new HashMap<>();

	private ExecutionPlanWithLock executionPlan;

	private CompletableFuture<Acknowledge> rescalePartitionFuture;

	private final ReconfigurationProfiler reconfigurationProfiler;

    /*

    // --------- JobManager --------

    private final LeaderRetrievalService jobManagerLeaderRetriever;

    */

	private final JobLeaderIdService jobLeaderIdService;

	private JobManagerRegistration jobManagerRegistration = null;

	private JobID jobId = null;

	public final static String CONTROLLER = "trisk.controller";

	// ------------------------------------------------------------------------

	public StreamManager(RpcService rpcService,
						 StreamManagerConfiguration streamManagerConfiguration,
						 ResourceID resourceId,
						 JobGraph jobGraph,
						 ClassLoader userCodeLoader,
						 HighAvailabilityServices highAvailabilityService,
						 JobLeaderIdService jobLeaderIdService,
						 LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
						 FatalErrorHandler fatalErrorHandler) throws Exception {
		super(rpcService, AkkaRpcServiceUtils.createRandomName(Stream_Manager_NAME), null);

		this.streamManagerConfiguration = checkNotNull(streamManagerConfiguration);
		this.resourceId = checkNotNull(resourceId);
		this.jobGraph = checkNotNull(jobGraph);
		this.userCodeLoader = checkNotNull(userCodeLoader);
		this.rpcTimeout = streamManagerConfiguration.getRpcTimeout();
		this.highAvailabilityServices = checkNotNull(highAvailabilityService);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
		this.jobLeaderIdService = checkNotNull(jobLeaderIdService);
		this.dispatcherGatewayRetriever = checkNotNull(dispatcherGatewayRetriever);

		final String jobName = jobGraph.getName();
		final JobID jid = jobGraph.getJobID();

		log.debug("Initializing sm for job {} ({})", jobName, jid);
		log.info("Initializing sm for job {} ({})", jobName, jid);

		this.jobGraphRescaler = new StreamJobGraphRescaler(jobGraph, userCodeLoader);

		/* now the policy is temporary hard coded added */
		String controllerName = streamManagerConfiguration.getConfiguration().getString(CONTROLLER, "DummyController");
		switch (controllerName) {
			case "DummyController":
				this.controlPolicyList.put("DummyController", new DummyController(this));
				break;
			case "StockController":
				this.controlPolicyList.put("StockController", new StockController(this, streamManagerConfiguration.getConfiguration()));
				break;
			case "NexmarkController":
				this.controlPolicyList.put("NexmarkController", new NexmarkController(this, streamManagerConfiguration.getConfiguration()));
				break;
			case "PerformanceEvaluator":
				this.controlPolicyList.put("PerformanceEvaluator", new PerformanceEvaluator(this, streamManagerConfiguration.getConfiguration()));
				break;
			case "FraudDetectionController":
				this.controlPolicyList.put("FraudDetectionController", new FraudDetectionController(this));
				break;
		}

		reconfigurationProfiler = new ReconfigurationProfiler(streamManagerConfiguration.getConfiguration());
	}

	/**
	 * Start the StreamManager service with the given {@link StreamManagerId}.
	 *
	 * @param newStreamManagerId to start the service with
	 * @return Future which is completed once the StreamManager service has been started
	 * @throws Exception if the StreamManager service could not be started
	 */
	@Override
	public CompletableFuture<Acknowledge> start(StreamManagerId newStreamManagerId) throws Exception {
		jobLeaderIdService.start(new JobLeaderIdActionsImpl());

		// make sure we receive RPC and async calls
		start();

		return callAsyncWithoutFencing(() -> startStreamManagement(newStreamManagerId), RpcUtils.INF_TIMEOUT);
	}

	/**
	 * Suspend the StreamManager service. This means that the service will stop to react
	 * to messages.
	 *
	 * @param cause for the suspension
	 * @return Future which is completed once the StreamManager service has been suspended
	 */
	@Override
	public CompletableFuture<Acknowledge> suspend(Exception cause) {
		CompletableFuture<Acknowledge> suspendFuture = callAsyncWithoutFencing(
			() -> suspendManagement(cause),
			RpcUtils.INF_TIMEOUT);
		return suspendFuture.whenComplete((acknowledge, throwable) -> stop());
	}

	// ------------------------------------------------------------------------
	//  RPC methods
	// ------------------------------------------------------------------------

	@Override
	public CompletableFuture<RegistrationResponse> registerJobManager(
		final JobMasterId jobMasterId,
		final ResourceID jobManagerResourceId,
		final String jobManagerAddress,
		final JobID jobId,
		final Time timeout) {

		checkNotNull(jobMasterId);
		checkNotNull(jobManagerResourceId);
		checkNotNull(jobManagerAddress);
		checkNotNull(jobId);

		if (!jobLeaderIdService.containsJob(jobId)) {
			try {
				jobLeaderIdService.addJob(jobId);
			} catch (Exception e) {
				StreamManagerException exception = new StreamManagerException("Could not add the job " +
					jobId + " to the job id leader service.", e);

				onFatalError(exception);
				log.error("Could not add job {} to job leader id service.", jobId, e);
				return FutureUtils.completedExceptionally(exception);
			}
		}

		log.info("Registering job manager {}@{} for job {}.", jobMasterId, jobManagerAddress, jobId);

		CompletableFuture<JobMasterId> jobMasterIdFuture;

		try {
			jobMasterIdFuture = jobLeaderIdService.getLeaderId(jobId);
		} catch (Exception e) {
			// we cannot check the job leader id so let's fail
			// TODO: Maybe it's also ok to skip this check in case that we cannot check the leader id
			StreamManagerException exception = new StreamManagerException("Cannot obtain the " +
				"job leader id future to verify the correct job leader.", e);

			onFatalError(exception);

			log.debug("Could not obtain the job leader id future to verify the correct job leader.");
			return FutureUtils.completedExceptionally(exception);
		}

		CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = getRpcService().connect(jobManagerAddress, jobMasterId, JobMasterGateway.class);

		CompletableFuture<RegistrationResponse> registrationResponseFuture = jobMasterGatewayFuture.thenCombineAsync(
			jobMasterIdFuture,
			(JobMasterGateway jobMasterGateway, JobMasterId leadingJobMasterId) -> {
				if (Objects.equals(leadingJobMasterId, jobMasterId)) {
					return registerJobMasterInternal(
						jobMasterGateway,
						jobId,
						jobManagerAddress,
						jobManagerResourceId);
				} else {
					final String declineMessage = String.format(
						"The leading JobMaster id %s did not match the received JobMaster id %s. " +
							"This indicates that a JobMaster leader change has happened.",
						leadingJobMasterId,
						jobMasterId);
					log.debug(declineMessage);
					return new RegistrationResponse.Decline(declineMessage);
				}
			},
			getMainThreadExecutor());

		// handle exceptions which might have occurred in one of the futures inputs of combine
		return registrationResponseFuture.handleAsync(
			(RegistrationResponse registrationResponse, Throwable throwable) -> {
				if (throwable != null) {
					if (log.isDebugEnabled()) {
						log.debug("Registration of job manager {}@{} failed.", jobMasterId, jobManagerAddress, throwable);
					} else {
						log.info("Registration of job manager {}@{} failed.", jobMasterId, jobManagerAddress);
					}

					return new RegistrationResponse.Decline(throwable.getMessage());
				} else {
					return registrationResponse;
				}
			},
			getRpcService().getExecutor());
	}

	@Override
	public void disconnectJobManager(JobID jobId, Exception cause) {
		closeJobManagerConnection(jobId, cause);
	}

	@Override
	public void rescaleStreamJob(JobRescaleAction.RescaleParamsWrapper wrapper) {
		validateRunsInMainThread();

//		checkState(this.rescalePartitionFuture.isDone(), "Last rescale/repartition action haven't done");
		Tuple2<List<JobVertexID>, List<JobVertexID>> involvedUpDownStream = null;
		switch (wrapper.type) {
			case SCALE_IN:
				involvedUpDownStream = jobGraphRescaler.repartition(
					wrapper.vertexID,
					wrapper.jobRescalePartitionAssignment.getPartitionAssignment());
				break;
			case SCALE_OUT:
				involvedUpDownStream = jobGraphRescaler.rescale(
					wrapper.vertexID,
					wrapper.newParallelism,
					wrapper.jobRescalePartitionAssignment.getPartitionAssignment());
				break;
			case REPARTITION:
				involvedUpDownStream = jobGraphRescaler.repartition(
					wrapper.vertexID,
					wrapper.jobRescalePartitionAssignment.getPartitionAssignment());
				break;
			default:
				log.warn("Not supported scale type:" + wrapper.type);
				return;
		}
		JobMasterGateway jobMasterGateway = this.jobManagerRegistration.getJobManagerGateway();
		Tuple2<List<JobVertexID>, List<JobVertexID>> upDownStream = involvedUpDownStream;
		runAsync(() -> jobMasterGateway.triggerJobRescale(wrapper, jobGraph, upDownStream.f0, upDownStream.f1));
	}

	public void rescale(int operatorID, int newParallelism, Map<Integer, List<Integer>> keyStateAllocation, ControlPolicy waitingController) {
		try {
			reconfigurationProfiler.onReconfigurationStart();
			// scale in is not support now
			checkState(keyStateAllocation.size() == newParallelism,
				"new parallelism not match key state allocation");
			this.executionPlan.setStateUpdatingFlag(waitingController);

			// operatorId to TaskIdList mapping, representing affected tasks.
			Map<Integer, List<Integer>> tasks = new HashMap<>();
			Map<Integer, List<Integer>> updateStateTasks = new HashMap<>();
			Map<Integer, List<Integer>> updateKeyMappingTasks = new HashMap<>();
			Map<Integer, List<Integer>> deployingTasks = new HashMap<>();

			OperatorDescriptor targetDescriptor = executionPlan.getOperatorByID(operatorID);
			// put tasks in target vertex
			tasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			updateStateTasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			deployingTasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			// although we put the update key mapping in the target operator, it will find the upstream tasks and update.
			updateKeyMappingTasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			// put tasks in upstream vertex
			targetDescriptor.getParents()
				.forEach(c -> {
					int operatorId = c.getOperatorID();
					List<Integer> curOpTasks = c.getTaskIds();
					tasks.put(operatorId, curOpTasks);
//					updateKeyMappingTasks.put(c.getOperatorID(), curOpTasks);
				});
			// put tasks in downstream vertex
			targetDescriptor.getChildren()
				.forEach(c -> {
					List<Integer> curOpTasks = c.getTaskIds();
					tasks.put(c.getOperatorID(), curOpTasks);
				});

			int oldParallelism = targetDescriptor.getParallelism();
			// update the parallelism
			targetDescriptor.setParallelism(newParallelism);
			boolean isScaleIn = oldParallelism > newParallelism;

			// update the key set
			for (OperatorDescriptor parent : targetDescriptor.getParents()) {
				parent.updateKeyMapping(operatorID, keyStateAllocation);
			}


			JobMasterGateway jobMasterGateway = this.jobManagerRegistration.getJobManagerGateway();
			final String PREPARE = "prepare timer";
			final String SYN = "synchronize timer";
			final String UPDATE_MAPPING = "updateKeyMapping timer";
			final String UPDATE_STATE = "updateState timer";
			log.info("++++++ start update");
			runAsync(() -> jobMasterGateway.callOperations(
				coordinator -> {
					// prepare andsynchronize among affected tasks
					CompletableFuture<?> syncFuture = FutureUtils.completedVoidFuture()
						.thenCompose(o -> {
							reconfigurationProfiler.onOtherStart(PREPARE);
							return coordinator.prepareExecutionPlan(executionPlan.getExecutionPlan());
						})
						.thenCompose(o -> {
							reconfigurationProfiler.onOtherEnd(PREPARE);
							reconfigurationProfiler.onOtherStart(SYN);
							return coordinator.synchronizeTasks(tasks, o);
						});
					// run update asynchronously.
					List<CompletableFuture<?>> updateFutureList = new ArrayList<>();
					updateFutureList.add(syncFuture.thenCompose(o -> {
						reconfigurationProfiler.onOtherEnd(SYN);
						reconfigurationProfiler.onOtherStart(UPDATE_MAPPING);
						return coordinator.updateKeyMapping(updateKeyMappingTasks, o);
					}));
					updateFutureList.add(syncFuture.thenCompose(o -> {
						reconfigurationProfiler.onOtherEnd(UPDATE_MAPPING);
						reconfigurationProfiler.onOtherStart(UPDATE_STATE);
						return coordinator.updateState(updateStateTasks, o);
					}));
					updateFutureList.add(syncFuture.thenCompose(o -> {
						return coordinator.updateTaskResources(deployingTasks, null);
					}));

					// finish the reconfiguration after all asynchronous update completed
					return FutureUtils.completeAll(updateFutureList)
						.thenCompose(o -> coordinator.resumeTasks())
						.whenComplete((o, failure) -> {
							if (failure != null) {
								LOG.error("Reconfiguration failed: ", failure);
								failure.printStackTrace();
							}
							try {
								System.out.println("++++++ finished update");
								log.info("++++++ finished update");
								// TODO: extract the deployment overhead
								reconfigurationProfiler.onOtherEnd(UPDATE_STATE);
								this.executionPlan.notifyUpdateFinished(failure);
								reconfigurationProfiler.onReconfigurationEnd();
							} catch (Exception e) {
								e.printStackTrace();
							}
						});
				}
			));
		} catch (Exception e) {
			LOG.error("Reconfiguration failed: ", e);
			e.printStackTrace();
		}
	}

	@Override
	public void rescale(ExecutionPlan executionPlan, int operatorID, Boolean isScaleIn,  ControlPolicy waitingController) {
		try {
			reconfigurationProfiler.onReconfigurationStart();
			// scale in is not support now
//			checkState(keyStateAllocation.size() == newParallelism,
//				"new parallelism not match key state allocation");
			executionPlan.getParallelism(operatorID);
			this.executionPlan.setStateUpdatingFlag(waitingController);

			// operatorId to TaskIdList mapping, representing affected tasks.
			Map<Integer, List<Integer>> tasks = new HashMap<>();
			Map<Integer, List<Integer>> updateStateTasks = new HashMap<>();
			Map<Integer, List<Integer>> updateKeyMappingTasks = new HashMap<>();
			Map<Integer, List<Integer>> deployingTasks = new HashMap<>();

			OperatorDescriptor targetDescriptor = this.executionPlan.getOperatorByID(operatorID);
			// put tasks in target vertex
			tasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			updateStateTasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			deployingTasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			// although we put the update key mapping in the target operator, it will find the upstream tasks and update.
			updateKeyMappingTasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			// put tasks in upstream vertex
			targetDescriptor.getParents()
				.forEach(c -> {
					int operatorId = c.getOperatorID();
					List<Integer> curOpTasks = c.getTaskIds();
					tasks.put(operatorId, curOpTasks);
//					updateKeyMappingTasks.put(c.getOperatorID(), curOpTasks);
				});
			// put tasks in downstream vertex
			targetDescriptor.getChildren()
				.forEach(c -> {
					List<Integer> curOpTasks = c.getTaskIds();
					tasks.put(c.getOperatorID(), curOpTasks);
				});

			JobMasterGateway jobMasterGateway = this.jobManagerRegistration.getJobManagerGateway();
			final String PREPARE = "prepare timer";
			final String SYN = "synchronize timer";
			final String UPDATE_MAPPING = "updateKeyMapping timer";
			final String UPDATE_STATE = "updateState timer";
			log.info("++++++ start update");
			runAsync(() -> jobMasterGateway.callOperations(
				coordinator -> {
					// prepare andsynchronize among affected tasks
					CompletableFuture<?> syncFuture = FutureUtils.completedVoidFuture()
						.thenCompose(o -> {
							reconfigurationProfiler.onOtherStart(PREPARE);
							return coordinator.prepareExecutionPlan(this.executionPlan.getExecutionPlan());
						})
						.thenCompose(o -> {
							reconfigurationProfiler.onOtherEnd(PREPARE);
							reconfigurationProfiler.onOtherStart(SYN);
							return coordinator.synchronizeTasks(tasks, o);
						});
					// run update asynchronously.
					List<CompletableFuture<?>> updateFutureList = new ArrayList<>();
					updateFutureList.add(syncFuture.thenCompose(o -> {
						reconfigurationProfiler.onOtherEnd(SYN);
						reconfigurationProfiler.onOtherStart(UPDATE_MAPPING);
						return coordinator.updateKeyMapping(updateKeyMappingTasks, o);
					}));
					updateFutureList.add(syncFuture.thenCompose(o -> {
						reconfigurationProfiler.onOtherEnd(UPDATE_MAPPING);
						reconfigurationProfiler.onOtherStart(UPDATE_STATE);
						return coordinator.updateState(updateStateTasks, o);
					}));
					updateFutureList.add(syncFuture.thenCompose(o -> {
						return coordinator.updateTaskResources(deployingTasks, null);
					}));

					// finish the reconfiguration after all asynchronous update completed
					return FutureUtils.completeAll(updateFutureList)
						.thenCompose(o -> coordinator.resumeTasks())
						.whenComplete((o, failure) -> {
							if (failure != null) {
								LOG.error("Reconfiguration failed: ", failure);
								failure.printStackTrace();
							}
							try {
								System.out.println("++++++ finished update");
								log.info("++++++ finished update");
								// TODO: extract the deployment overhead
								reconfigurationProfiler.onOtherEnd(UPDATE_STATE);
								this.executionPlan.notifyUpdateFinished(failure);
								reconfigurationProfiler.onReconfigurationEnd();
							} catch (Exception e) {
								e.printStackTrace();
							}
						});
				}
			));
		} catch (Exception e) {
			LOG.error("Reconfiguration failed: ", e);
			e.printStackTrace();
		}
	}

	@Override
	public void placement(int operatorID, Map<Integer, List<Integer>> keyStateAllocation, ControlPolicy waitingController) {
		try {
			reconfigurationProfiler.onReconfigurationStart();
			executionPlan.setStateUpdatingFlag(waitingController);

			// operatorId to TaskIdList mapping, representing affected tasks.
			Map<Integer, List<Integer>> tasks = new HashMap<>();
			Map<Integer, List<Integer>> updateKeyMappingTasks = new HashMap<>();
			Map<Integer, List<Integer>> deployingTasks = new HashMap<>();

			OperatorDescriptor targetDescriptor = executionPlan.getOperatorByID(operatorID);
			// put tasks in target vertex
			tasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			deployingTasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			// although we put the update key mapping in the target operator, it will find the upstream tasks and update.
			updateKeyMappingTasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			// put tasks in upstream vertex
			targetDescriptor.getParents()
				.forEach(c -> {
					int operatorId = c.getOperatorID();
					List<Integer> curOpTasks = c.getTaskIds();
					tasks.put(operatorId, curOpTasks);
//					updateKeyMappingTasks.put(c.getOperatorID(), curOpTasks);
				});
			// put tasks in downstream vertex
			targetDescriptor.getChildren()
				.forEach(c -> {
					List<Integer> curOpTasks = c.getTaskIds();
					tasks.put(c.getOperatorID(), curOpTasks);
				});

			// update the key set
			for (OperatorDescriptor parent : targetDescriptor.getParents()) {
				parent.updateKeyMapping(operatorID, keyStateAllocation);
			}


			JobMasterGateway jobMasterGateway = this.jobManagerRegistration.getJobManagerGateway();
			final String PREPARE = "prepare timer";
			final String SYN = "synchronize timer";
			final String UPDATE_MAPPING = "updateKeyMapping timer";
			final String UPDATE_STATE = "updateState timer";
			log.info("++++++ start update");
			runAsync(() -> jobMasterGateway.callOperations(
				coordinator -> {
					// prepare andsynchronize among affected tasks
					CompletableFuture<?> syncFuture = FutureUtils.completedVoidFuture()
						.thenCompose(o -> {
							reconfigurationProfiler.onOtherStart(PREPARE);
							return coordinator.prepareExecutionPlan(executionPlan.getExecutionPlan());
						})
						.thenCompose(o -> {
							reconfigurationProfiler.onOtherEnd(PREPARE);
							reconfigurationProfiler.onOtherStart(SYN);
							return coordinator.synchronizeTasks(tasks, o);
						});
					// run update asynchronously.
					List<CompletableFuture<?>> updateFutureList = new ArrayList<>();
					updateFutureList.add(syncFuture.thenCompose(o -> {
						reconfigurationProfiler.onOtherEnd(SYN);
						reconfigurationProfiler.onOtherStart(UPDATE_MAPPING);
						return coordinator.updateKeyMapping(updateKeyMappingTasks, o);
					}));
//					updateFutureList.add(syncFuture.thenCompose(o -> {
//						reconfigurationProfiler.onOtherEnd(UPDATE_MAPPING);
//						reconfigurationProfiler.onOtherStart(UPDATE_STATE);
//						return coordinator.updateState(updateStateTasks, o);
//					}));
					updateFutureList.add(syncFuture
						.thenCompose(o -> coordinator.updateTaskResources(deployingTasks, null)));
//					updateFutureList.add(syncFuture.thenCompose(o -> {
//						return coordinator.updateTaskResources(deployingTasks, true);
//					}));

					// finish the reconfiguration after all asynchronous update completed
					return FutureUtils.completeAll(updateFutureList)
						.thenCompose(o -> coordinator.resumeTasks())
						.whenComplete((o, failure) -> {
							if (failure != null) {
								LOG.error("Reconfiguration failed: ", failure);
								failure.printStackTrace();
							}
							try {
								System.out.println("++++++ finished update");
								log.info("++++++ finished update");
								// TODO: extract the deployment overhead
								this.executionPlan.notifyUpdateFinished(failure);
								reconfigurationProfiler.onReconfigurationEnd();
							} catch (Exception e) {
								e.printStackTrace();
							}
						});
				}
			));
		} catch (Exception e) {
			LOG.error("Reconfiguration failed: ", e);
			e.printStackTrace();
		}
	}

	@Override
	public void rebalance(int operatorID, Map<Integer, List<Integer>> keyStateAllocation, boolean stateful, ControlPolicy waitingController) {
		try {
			reconfigurationProfiler.onReconfigurationStart();
			// typically, the target operator should contain key state,
			// todo if keyStateAllocation is null, means it is stateless operator, but not support now
			this.executionPlan.setStateUpdatingFlag(waitingController);

			OperatorDescriptor targetDescriptor = executionPlan.getOperatorByID(operatorID);

			for (OperatorDescriptor parent : targetDescriptor.getParents()) {
				parent.updateKeyMapping(operatorID, keyStateAllocation);
			}

			// operatorId to TaskIdList mapping, representing affected tasks.
			Map<Integer, List<Integer>> tasks = new HashMap<>();
			Map<Integer, List<Integer>> updateStateTasks = new HashMap<>();
			Map<Integer, List<Integer>> updateKeyMappingTasks = new HashMap<>();

			// put tasks in target vertex
			tasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			updateStateTasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			// although we put the update key mapping in the target operator, it will find the upstream tasks and update.
			updateKeyMappingTasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());

			// put tasks in upstream vertex
			targetDescriptor.getParents()
				.forEach(c -> {
					int operatorId = c.getOperatorID();
					List<Integer> curOpTasks = c.getTaskIds();
					tasks.put(operatorId, curOpTasks);
				});

			JobMasterGateway jobMasterGateway = this.jobManagerRegistration.getJobManagerGateway();
			final String PREPARE = "prepare timer";
			final String SYN = "synchronize timer";
			final String UPDATE_MAPPING = "updateKeyMapping timer";
			final String UPDATE_STATE = "updateState timer";
			if (stateful) {
				runAsync(() -> jobMasterGateway.callOperations(
				coordinator -> {
					// prepare and synchronize among affected tasks
					CompletableFuture<?> syncFuture = FutureUtils.completedVoidFuture()
						.thenCompose(o -> {
							reconfigurationProfiler.onOtherStart(PREPARE);
							return coordinator.prepareExecutionPlan(executionPlan.getExecutionPlan());
						})
						.thenCompose(o -> {
							reconfigurationProfiler.onOtherEnd(PREPARE);
							reconfigurationProfiler.onOtherStart(SYN);
							return coordinator.synchronizeTasks(tasks, o);
						});
					// run update asynchronously.
					List<CompletableFuture<?>> updateFutureList = new ArrayList<>();
					updateFutureList.add(syncFuture.thenCompose(o -> {
						reconfigurationProfiler.onOtherEnd(SYN);
						reconfigurationProfiler.onOtherStart(UPDATE_MAPPING);
						return coordinator.updateKeyMapping(updateKeyMappingTasks, o);
					})
//						.thenCompose(o -> {
//						reconfigurationProfiler.onOtherEnd(UPDATE_MAPPING);
//						return CompletableFuture.completedFuture(o); })
					);
					updateFutureList.add(syncFuture.thenCompose(o -> {
						reconfigurationProfiler.onOtherEnd(UPDATE_MAPPING);
						reconfigurationProfiler.onOtherStart(UPDATE_STATE);
						return coordinator.updateState(updateStateTasks, o);
					}).thenCompose(o -> {
						reconfigurationProfiler.onOtherEnd(UPDATE_STATE);
						return CompletableFuture.completedFuture(o);
					}));
					// finish the reconfiguration after all asynchronous update completed
					return FutureUtils.completeAll(updateFutureList)
//						.thenCompose(o -> coordinator.resumeTasks()) // resume all tasks who has both state and mapping to update (to be appeared in the future).
						.whenComplete((o, failure) -> {
						if (failure != null) {
							failure.printStackTrace();
						}
						try {
							System.out.println("++++++ finished update");
							LOG.info("++++++ finished update");
							reconfigurationProfiler.onReconfigurationEnd();
							this.executionPlan.notifyUpdateFinished(failure);
						} catch (Exception e) {
							e.printStackTrace();
						}
					});
				}));
			} else {
				runAsync(() -> jobMasterGateway.callOperations(
					coordinator -> FutureUtils.completedVoidFuture()
						.thenCompose(o -> coordinator.prepareExecutionPlan(executionPlan.getExecutionPlan()))
						.thenCompose(o -> coordinator.updateKeyMapping(operatorID, o))
						.whenComplete((o, failure) -> {
							if (failure != null) {
								failure.printStackTrace();
							}
							try {
								reconfigurationProfiler.onReconfigurationEnd();
								this.executionPlan.notifyUpdateFinished(failure);
							} catch (Exception e) {
								e.printStackTrace();
							}
						})
				));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void rebalance(ExecutionPlan executionPlan, int operatorID, ControlPolicy waitingController) {
		try {
			reconfigurationProfiler.onReconfigurationStart();
			// typically, the target operator should contain key state,
			// todo if keyStateAllocation is null, means it is stateless operator, but not support now
			this.executionPlan.setStateUpdatingFlag(waitingController);

			OperatorDescriptor targetDescriptor = executionPlan.getOperatorByID(operatorID);
			Map<Integer, List<Integer>> keyStateAllocation = executionPlan.getKeyStateAllocation(operatorID);

			for (OperatorDescriptor parent : targetDescriptor.getParents()) {
				parent.updateKeyMapping(operatorID, keyStateAllocation);
			}

			// operatorId to TaskIdList mapping, representing affected tasks.
			Map<Integer, List<Integer>> tasks = new HashMap<>();
			Map<Integer, List<Integer>> updateStateTasks = new HashMap<>();
			Map<Integer, List<Integer>> updateKeyMappingTasks = new HashMap<>();

			// put tasks in target vertex
			tasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			updateStateTasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());
			// although we put the update key mapping in the target operator, it will find the upstream tasks and update.
			updateKeyMappingTasks.put(targetDescriptor.getOperatorID(), targetDescriptor.getTaskIds());

			// put tasks in upstream vertex
			targetDescriptor.getParents()
				.forEach(c -> {
					int operatorId = c.getOperatorID();
					List<Integer> curOpTasks = c.getTaskIds();
					tasks.put(operatorId, curOpTasks);
				});

			JobMasterGateway jobMasterGateway = this.jobManagerRegistration.getJobManagerGateway();
			final String PREPARE = "prepare timer";
			final String SYN = "synchronize timer";
			final String UPDATE_MAPPING = "updateKeyMapping timer";
			final String UPDATE_STATE = "updateState timer";
			runAsync(() -> jobMasterGateway.callOperations(
				coordinator -> {
					// prepare and synchronize among affected tasks
					CompletableFuture<?> syncFuture = FutureUtils.completedVoidFuture()
						.thenCompose(o -> {
							reconfigurationProfiler.onOtherStart(PREPARE);
							return coordinator.prepareExecutionPlan(this.executionPlan.getExecutionPlan());
						})
						.thenCompose(o -> {
							reconfigurationProfiler.onOtherEnd(PREPARE);
							reconfigurationProfiler.onOtherStart(SYN);
							return coordinator.synchronizeTasks(tasks, o);
						});
					// run update asynchronously.
					List<CompletableFuture<?>> updateFutureList = new ArrayList<>();
					updateFutureList.add(syncFuture.thenCompose(o -> {
							reconfigurationProfiler.onOtherEnd(SYN);
							reconfigurationProfiler.onOtherStart(UPDATE_MAPPING);
							return coordinator.updateKeyMapping(updateKeyMappingTasks, o);
						})
//						.thenCompose(o -> {
//						reconfigurationProfiler.onOtherEnd(UPDATE_MAPPING);
//						return CompletableFuture.completedFuture(o); })
					);
					updateFutureList.add(syncFuture.thenCompose(o -> {
						reconfigurationProfiler.onOtherEnd(UPDATE_MAPPING);
						reconfigurationProfiler.onOtherStart(UPDATE_STATE);
						return coordinator.updateState(updateStateTasks, o);
					}).thenCompose(o -> {
						reconfigurationProfiler.onOtherEnd(UPDATE_STATE);
						return CompletableFuture.completedFuture(o);
					}));
					// finish the reconfiguration after all asynchronous update completed
					return FutureUtils.completeAll(updateFutureList)
//						.thenCompose(o -> coordinator.resumeTasks()) // resume all tasks who has both state and mapping to update (to be appeared in the future).
						.whenComplete((o, failure) -> {
							if (failure != null) {
								failure.printStackTrace();
							}
							try {
								System.out.println("++++++ finished update");
								LOG.info("++++++ finished update");
								reconfigurationProfiler.onReconfigurationEnd();
								this.executionPlan.notifyUpdateFinished(failure);
							} catch (Exception e) {
								e.printStackTrace();
							}
						});
				}));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void reconfigureUserFunction(int operatorID, Object function, ControlPolicy waitingController) {
		try {
			// very similar to acquire a positive spin write lock
			this.executionPlan.setStateUpdatingFlag(waitingController);
			OperatorDescriptor target = executionPlan.getOperatorByID(operatorID);
			target.setControlAttribute(UDF, function);
			final String PREPARE = "prepare timer";
			final String SYN = "synchronize timer";
			final String UPDATE_FUNCTION = "updateFunction timer";
			JobMasterGateway jobMasterGateway = this.jobManagerRegistration.getJobManagerGateway();
			runAsync(() -> jobMasterGateway.callOperations(
				coordinator -> FutureUtils.completedVoidFuture()
					.thenCompose(o -> {
						reconfigurationProfiler.onReconfigurationStart();
						reconfigurationProfiler.onOtherStart(PREPARE);
						return coordinator.prepareExecutionPlan(executionPlan.getExecutionPlan());
					})
					.thenCompose(o -> {
						reconfigurationProfiler.onOtherEnd(PREPARE);
						reconfigurationProfiler.onOtherStart(SYN);
						return coordinator.synchronizeTasks(Collections.singletonList(Tuple2.of(operatorID, -1)), o);
					})
					.thenCompose(o -> {
						reconfigurationProfiler.onOtherEnd(SYN);
						reconfigurationProfiler.onOtherStart(UPDATE_FUNCTION);
						return coordinator.updateFunction(operatorID, o);
					})
					.whenComplete((o, failure) -> {
						if (failure != null) {
							failure.printStackTrace();
						}
						try {
							reconfigurationProfiler.onOtherEnd(UPDATE_FUNCTION);
							reconfigurationProfiler.onReconfigurationEnd();
							this.executionPlan.notifyUpdateFinished(failure);
						} catch (Exception e) {
							e.printStackTrace();
						}
					})
			));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void noOp(int operatorID, ControlPolicy waitingController) {
		try {
			// very similar to acquire a positive spin write lock
			this.executionPlan.setStateUpdatingFlag(waitingController);
			JobMasterGateway jobMasterGateway = this.jobManagerRegistration.getJobManagerGateway();
			final String SYN = "synchronize timer";
			runAsync(() -> jobMasterGateway.callOperations(
				coordinator -> FutureUtils.completedVoidFuture()
					.thenCompose(o -> {
						reconfigurationProfiler.onOtherStart(SYN);
						return coordinator.synchronizeTasks(Collections.singletonList(Tuple2.of(operatorID, -1)), null);
					})
					.thenCompose(o -> {
						reconfigurationProfiler.onOtherEnd(SYN);
						return coordinator.resumeTasks();
					})
					.whenComplete((o, failure) -> {
						if (failure != null) {
							failure.printStackTrace();
						}
						try {
							this.executionPlan.notifyUpdateFinished(failure);
						} catch (Exception e) {
							e.printStackTrace();
						}
					})
			));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void callCustomizeOperations(Function<PrimitiveOperation<Map<Integer, Map<Integer, AbstractCoordinator.Diff>>>, CompletableFuture<?>> operationCaller) {
		try {
			JobMasterGateway jobMasterGateway = this.jobManagerRegistration.getJobManagerGateway();
			runAsync(() -> jobMasterGateway.callOperations(operationCaller));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//----------------------------------------------------------------------------------------------
	// Internal methods
	//----------------------------------------------------------------------------------------------

	@Override
	public void streamSwitchCompleted(JobVertexID targetVertexID) {
		for (ControlPolicy policy : controlPolicyList.values()) {
			policy.onChangeCompleted(null);
		}
	}

	@Override
	public void jobStatusChanged(JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error, ExecutionPlan jobAbstraction) {
		runAsync(
			() -> {
				if (jobAbstraction != null) {
					this.executionPlan = new ExecutionPlanWithLock(jobAbstraction);
				}
				if (newJobStatus == JobStatus.RUNNING) {
					for (ControlPolicy policy : controlPolicyList.values()) {
						policy.startControllers();
					}
				} else {
					for (ControlPolicy policy : controlPolicyList.values()) {
						policy.stopControllers();
					}
				}
			}
		);
	}

	@Override
	public ExecutionPlanAndJobGraphUpdaterFactory getStreamRelatedInstanceFactory() {
		return DefaultExecutionPlanAndJobGraphUpdaterFactory.INSTANCE;
	}

	public boolean registerNewController(String controllerID, String className, String sourceCode){
		ControlPolicy newController = null;
		try {
			byte[] classMetaData = ByteClassLoader.compileJavaClass(className, sourceCode);
			Class<? extends AbstractController> controllerClass = ByteClassLoader.loadClassFromByteArray(classMetaData, className);
			Constructor<? extends AbstractController> constructor= controllerClass.getConstructor(ReconfigurationExecutor.class);
			newController = constructor.newInstance(this);
		} catch (IOException | ClassNotFoundException | NoSuchMethodException
			| InstantiationException | IllegalAccessException | InvocationTargetException e) {
			e.printStackTrace();
			return false;
		}
		ControlPolicy oldControlPolicy = this.controlPolicyList.put(controllerID, newController);
		newController.startControllers();
		if(oldControlPolicy != null){
			oldControlPolicy.stopControllers();
			return true;
		}
		return false;
	}

	/**
	 * Registers a new JobMaster.
	 *
	 * @param jobMasterGateway     to communicate with the registering JobMaster
	 * @param jobId                of the job for which the JobMaster is responsible
	 * @param jobManagerAddress    address of the JobMaster
	 * @param jobManagerResourceId ResourceID of the JobMaster
	 * @return RegistrationResponse
	 */
	private RegistrationResponse registerJobMasterInternal(
		final JobMasterGateway jobMasterGateway,
		JobID jobId,
		String jobManagerAddress,
		ResourceID jobManagerResourceId) {
		if (jobManagerRegistration != null) {
			JobManagerRegistration oldJobManagerRegistration = jobManagerRegistration;
			if (Objects.equals(oldJobManagerRegistration.getJobMasterId(), jobMasterGateway.getFencingToken())) {
				// same registration
				log.debug("Job manager {}@{} was already registered.", jobMasterGateway.getFencingToken(), jobManagerAddress);
			} else {
				disconnectJobManager(
					oldJobManagerRegistration.getJobID(),
					new Exception("New job leader for job " + jobId));

				this.jobManagerRegistration = new JobManagerRegistration(
					jobId,
					jobManagerResourceId,
					jobMasterGateway);
				this.jobId = jobId;
			}
		} else {
			this.jobManagerRegistration = new JobManagerRegistration(
				jobId,
				jobManagerResourceId,
				jobMasterGateway);
			this.jobId = jobId;
		}

		log.info("Registered job manager {}@{} for job {}.", jobMasterGateway.getFencingToken(), jobManagerAddress, jobId);

		// TODO: HeartBeatService

		return new JobMasterRegistrationSuccess<StreamManagerId>(
			getFencingToken(),
			resourceId);
	}

	protected void closeJobManagerConnection(JobID jobID, Exception cause) {
		// TODO: To be implemented
	}

	private Acknowledge startStreamManagement(StreamManagerId newStreamManagerId) throws Exception {

		validateRunsInMainThread();

		OptionalConsumer<DispatcherGateway> optLeaderConsumer = OptionalConsumer.of(this.dispatcherGatewayRetriever.getNow());

		optLeaderConsumer.ifPresent(
			gateway -> {
				try {
					log.info("connect dispatcher gateway successfully");
					// todo, how to know job success run so we can start something related control plane (eg. streamSwitch)
					gateway.submitJob(jobGraph, this.getAddress(), Time.seconds(10));
				} catch (Exception e) {
					log.error("Error while invoking runtime dispatcher RMI.", e);
				}
			}
		).ifNotPresent(
			() ->
				log.error("Error while connecting runtime dispatcher."));

		checkNotNull(newStreamManagerId, "The new StreamManagerId must not be null");

		setFencingToken(newStreamManagerId);

		return Acknowledge.get();
	}

	/**
	 * Suspending stream manager, (cancel the job, to be consider), and other communication with other components
	 * will be disposed.
	 *
	 * @param cause The reason of why this stream manger been suspended.
	 */
	private Acknowledge suspendManagement(final Exception cause) {
		validateRunsInMainThread();

		if (getFencingToken() == null) {
			log.debug("Stream Management has already benn suspended or shutdown.");
			return Acknowledge.get();
		}

		// not leader anymore --> set the StreamManagerId to null
		setFencingToken(null);

		// TODO:
		// closeJobManagerConnection(cause);

		// stop other services

		return Acknowledge.get();
	}

	//----------------------------------------------------------------------------------------------
	// Service methods
	//----------------------------------------------------------------------------------------------

	/**
	 * Get the {@link StreamManagerGateway} belonging to this service.
	 *
	 * @return StreamManagerGateway belonging to this service
	 */
	@Override
	public StreamManagerGateway getGateway() {
		return getSelfGateway(StreamManagerGateway.class);
	}

	@Override
	public ExecutionPlan getExecutionPlan() {
		return checkNotNull(executionPlan.getExecutionPlan(), "stream job abstraction (execution plan) have not been initialized");
	}

	@Override
	public ExecutionPlanWithLock getExecutionPlanCopy() {
//		ExecutionPlan executionPlanCopy = new ExecutionPlanImpl();
//		ExecutionPlanWithLock executionPlanWithLockCopy = new ExecutionPlanWithLock(executionPlan.getExecutionPlan());
		return executionPlan.copy();
	}

	@Override
	public Configuration getExperimentConfig(){
		return this.streamManagerConfiguration.getConfiguration();
	}

	@Override
	public void execute(ControlPolicy controller, ExecutionPlanWithLock executionPlanCopy) {
		try {
			executionPlan = executionPlanCopy;
			executionPlan.setStateUpdatingFlag(controller);

			Map<String, Map<Integer, List<Integer>>> transformations = executionPlan.getTransformations();

			// operatorId to TaskIdList mapping, representing affected tasks.
			boolean isScaleIn = false;
			Map<Integer, List<Integer>> tasks = new HashMap<>();
			Map<Integer, List<Integer>> updateStateTasks = new HashMap<>();
			Map<Integer, List<Integer>> updateKeyMappingTasks = new HashMap<>();
			Map<Integer, List<Integer>> reDeployingTasks = new HashMap<>();
			Map<Integer, List<Integer>> updateFunctionTasks = new HashMap<>();
			Map<Integer, List<SlotID>> targetSlotAllocation = null;
			for (String operation : transformations.keySet()) {
				Map<Integer, List<Integer>> transformation = transformations.get(operation);
				switch (operation) {
					case "redistribute":
						// TODO: by far, we find the upstream operators of target operator to do remap, should update to only use "remapping"
						updateStateTasks = transformation;
						break;
					case "redeploying":
						targetSlotAllocation = executionPlan.getSlotAllocation();
						reDeployingTasks = transformation;
						break;
					case "remapping":
						updateKeyMappingTasks = transformation;
						break;
					case "updateExecutionLogic":
						updateFunctionTasks = transformation;
						break;
				}
				// tips: downstream tasks can be placed here when doing rescale
				for (Integer operatorID : transformation.keySet()) {
					tasks.putIfAbsent(operatorID, transformation.get(operatorID));
				}
			}

			JobMasterGateway jobMasterGateway = this.jobManagerRegistration.getJobManagerGateway();
			log.info("++++++ start update");
			Map<Integer, List<Integer>> finalUpdateKeyMappingTasks = updateKeyMappingTasks;
			Map<Integer, List<Integer>> finalUpdateStateTasks = updateStateTasks;
			Map<Integer, List<Integer>> finalDeployingTasks = reDeployingTasks;
			Map<Integer, List<Integer>> finalUpdateFunctionTasks = updateFunctionTasks;
			Map<Integer, List<SlotID>> finalSlotAllocation = targetSlotAllocation;
			runAsync(() -> jobMasterGateway.callOperations(
				coordinator -> {
					// prepare and synchronize among affected tasks
					CompletableFuture<?> syncFuture = FutureUtils.completedVoidFuture()
						.thenCompose(o -> coordinator.prepareExecutionPlan(executionPlan.getExecutionPlan()))
						.thenCompose(o -> coordinator.synchronizeTasks(tasks, o));
					// run update asynchronously.
					List<CompletableFuture<?>> updateFutureList = new ArrayList<>();
					// TODO: by far, our method only support one operator, need to extend to support multiple operators.
					if (!finalUpdateKeyMappingTasks.isEmpty()) {
						updateFutureList.add(syncFuture.thenCompose(o -> coordinator.updateKeyMapping(finalUpdateKeyMappingTasks, o)));
					}
					if (!finalUpdateStateTasks.isEmpty()) {
						updateFutureList.add(syncFuture.thenCompose(o -> coordinator.updateState(finalUpdateStateTasks, o)));
					}
					if (!finalDeployingTasks.isEmpty()) {
						updateFutureList.add(syncFuture.thenCompose(o -> coordinator.updateTaskResources(finalDeployingTasks, finalSlotAllocation)));
					}
					if (!finalUpdateFunctionTasks.isEmpty()) {
						updateFutureList.add(syncFuture.thenCompose(o -> coordinator.updateFunction(finalUpdateFunctionTasks, o)));
					}

					// finish the reconfiguration after all asynchronous update completed
					CompletableFuture<Void> finishFuture = FutureUtils.completeAll(updateFutureList)
						.thenCompose(o -> coordinator.resumeTasks());
					if (finalSlotAllocation != null) {
						finishFuture = finishFuture
							.thenCompose(o -> jobMasterGateway.getAllSlots())
							.thenAccept(taskManagerSlots -> {
								// compute
								Map<String, List<AbstractSlot>> slotMap = new HashMap<>();
								for (TaskManagerSlot taskManagerSlot : taskManagerSlots) {
									AbstractSlot slot = FlinkSlot.fromTaskManagerSlot(taskManagerSlot);
									List<AbstractSlot> slots = slotMap.computeIfAbsent(slot.getLocation(), k -> new ArrayList<>());
									slots.add(slot);
								}
								executionPlan.getExecutionPlan().setSlotMap(slotMap);
							});
					}
					return finishFuture.whenComplete((o, failure) -> {
						if (failure != null) {
							LOG.error("Reconfiguration failed: ", failure);
							failure.printStackTrace();
						}
						try {
							System.out.println("++++++ finished update");
							log.info("++++++ finished update");
							executionPlan.clearTransformations();
							executionPlan.notifyUpdateFinished(failure);
						} catch (Exception e) {
							e.printStackTrace();
						}
					});
				})
			);
		} catch (Exception e) {
			LOG.error("Reconfiguration failed: ", e);
			e.printStackTrace();
		}
	}

	// ------------------------------------------------------------------------
	//  Error Handling
	// ------------------------------------------------------------------------

	/**
	 * Notifies the ResourceManager that a fatal error has occurred and it cannot proceed.
	 *
	 * @param t The exception describing the fatal error
	 */
	protected void onFatalError(Throwable t) {
		try {
			log.error("Fatal error occurred in ResourceManager.", t);
		} catch (Throwable ignored) {
		}

		// The fatal error handler implementation should make sure that this call is non-blocking
		fatalErrorHandler.onFatalError(t);
	}

	protected void jobLeaderLostLeadership(JobID jobId, JobMasterId oldJobMasterId) {
		if (jobId == this.jobId) {

			if (Objects.equals(jobManagerRegistration.getJobMasterId(), oldJobMasterId)) {
				disconnectJobManager(jobId, new Exception("Job leader lost leadership."));
			} else {
				log.debug("Discarding job leader lost leadership, because a new job leader was found for job {}. ", jobId);
			}
		} else {
			log.debug("Discard job leader lost leadership for outdated leader {} for job {}.", oldJobMasterId, jobId);
		}
	}

	protected void removeJob(JobID jobId) {
		try {
			jobLeaderIdService.removeJob(jobId);
		} catch (Exception e) {
			log.warn("Could not properly remove the job {} from the job leader id service.", jobId, e);
		}

		if (jobId == this.jobId) {
			disconnectJobManager(jobId, new Exception("Job " + jobId + "was removed"));
		}
	}


	private class JobLeaderIdActionsImpl implements JobLeaderIdActions {

		@Override
		public void jobLeaderLostLeadership(final JobID jobId, final JobMasterId oldJobMasterId) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					StreamManager.this.jobLeaderLostLeadership(jobId, oldJobMasterId);
				}
			});
		}

		@Override
		public void notifyJobTimeout(final JobID jobId, final UUID timeoutId) {
			runAsync(new Runnable() {
				@Override
				public void run() {
					if (jobLeaderIdService.isValidTimeout(jobId, timeoutId)) {
						removeJob(jobId);
					}
				}
			});
		}

		@Override
		public void handleError(Throwable error) {
			onFatalError(error);
		}
	}
}
