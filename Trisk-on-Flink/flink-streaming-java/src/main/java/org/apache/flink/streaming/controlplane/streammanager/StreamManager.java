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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerId;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerGateway;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdActions;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.registration.JobManagerRegistration;
import org.apache.flink.runtime.rpc.*;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.streaming.controlplane.streammanager.exceptions.StreamManagerException;
import org.apache.flink.util.OptionalConsumer;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author trx
 * StreamManager implementation.
 * <p>
 * TODO:
 * 1. decide other fields
 * 2. initialize other fields
 */
public class StreamManager extends FencedRpcEndpoint<StreamManagerId> implements StreamManagerGateway, StreamManagerService {

	/**
	 * Default names for Flink's distributed components.
	 */
	public static final String Stream_Manager_NAME = "streammanager";

	private final StreamManagerConfiguration streamManagerConfiguration;

	private final ResourceID resourceId;

	private final JobGraph jobGraph;

	private final Time rpcTimeout;

	private final HighAvailabilityServices highAvailabilityServices;

	private final FatalErrorHandler fatalErrorHandler;

	private final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever;

    /*

    // --------- JobManager --------

    private final LeaderRetrievalService jobManagerLeaderRetriever;

    */

	private final JobLeaderIdService jobLeaderIdService;

	private JobManagerRegistration jobManagerRegistration = null;

	private JobID jobId = null;

	// ------------------------------------------------------------------------

	public StreamManager(RpcService rpcService,
						 StreamManagerConfiguration streamManagerConfiguration,
						 ResourceID resourceId,
						 JobGraph jobGraph,
						 HighAvailabilityServices highAvailabilityService,
						 JobLeaderIdService jobLeaderIdService,
						 LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
						 FatalErrorHandler fatalErrorHandler) throws Exception {
		super(rpcService, AkkaRpcServiceUtils.createRandomName(Stream_Manager_NAME), null);

		this.streamManagerConfiguration = checkNotNull(streamManagerConfiguration);
		this.resourceId = checkNotNull(resourceId);
		this.jobGraph = checkNotNull(jobGraph);
		this.rpcTimeout = streamManagerConfiguration.getRpcTimeout();
		this.highAvailabilityServices = checkNotNull(highAvailabilityService);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
		this.jobLeaderIdService = checkNotNull(jobLeaderIdService);
		this.dispatcherGatewayRetriever = checkNotNull(dispatcherGatewayRetriever);

		final String jobName = jobGraph.getName();
		final JobID jid = jobGraph.getJobID();

		log.debug("Initializing sm for job {} ({})", jobName, jid);
		log.info("Initializing sm for job {} ({})", jobName, jid);

        /*
        TODO: initialize other fields
         */
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
		return suspendFuture.whenComplete(((acknowledge, throwable) -> stop()));
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


	//----------------------------------------------------------------------------------------------
	// Internal methods
	//----------------------------------------------------------------------------------------------


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
					log.info("connect successfully");
					gateway.submitJob(jobGraph,
						this.getAddress(),
						Time.seconds(10));
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
	public StreamManager getGateway() {
		return getSelfGateway(StreamManager.class);
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
