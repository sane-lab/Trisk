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

package org.apache.flink.streaming.controlplane.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerRunner;
import org.apache.flink.runtime.dispatcher.DispatcherException;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerRunnerImpl;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.PermanentlyFencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.CheckedSupplier;
import org.apache.flink.util.function.FunctionUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;

/**
 * Base class for the Dispatcher component. The Dispatcher component is responsible
 * for receiving job submissions, persisting them, spawning JobManagers to execute
 * the jobs and to recover them in case of a master failure. Furthermore, it knows
 * about the state of the Flink session cluster.
 */
public abstract class StreamManagerDispatcher extends PermanentlyFencedRpcEndpoint<StreamManagerDispatcherId> implements StreamManagerDispatcherGateway {

	public static final String DISPATCHER_NAME = "smDispatcher";

	private final Configuration configuration;

	private final JobGraphWriter jobGraphWriter;
	private final RunningJobsRegistry runningJobsRegistry;

	private final HighAvailabilityServices highAvailabilityServices;
	private final JobManagerSharedServices jobManagerSharedServices;
	private final BlobServer blobServer;

	private final FatalErrorHandler fatalErrorHandler;

	private final Map<JobID, CompletableFuture<JobManagerRunner>> jobManagerRunnerFutures;

	private final Collection<JobGraph> recoveredJobs;


	private StreamManagerRunnerFactory streamManagerRunnerFactory;

	private final Map<JobID, CompletableFuture<Void>> jobManagerTerminationFutures;

	protected final CompletableFuture<ApplicationStatus> shutDownFuture;

	private final LeaderRetrievalService dispatcherLeaderRetrievalService;

	private final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever;


	public StreamManagerDispatcher(
		RpcService rpcService,
		String endpointId,
		StreamManagerDispatcherId fencingToken,
		Collection<JobGraph> recoveredJobs,
		StreamManagerDispatcherServices dispatcherServices) throws Exception {
		super(rpcService, endpointId, fencingToken);
		Preconditions.checkNotNull(dispatcherServices);

		this.configuration = dispatcherServices.getConfiguration();
		this.highAvailabilityServices = dispatcherServices.getHighAvailabilityServices();
		this.blobServer = dispatcherServices.getBlobServer();
		this.fatalErrorHandler = dispatcherServices.getFatalErrorHandler();
		this.jobGraphWriter = dispatcherServices.getJobGraphWriter();

		this.jobManagerSharedServices = JobManagerSharedServices.fromConfiguration(
			configuration,
			blobServer);

		this.runningJobsRegistry = highAvailabilityServices.getRunningJobsRegistry();

		jobManagerRunnerFutures = new HashMap<>(16);

		this.streamManagerRunnerFactory = dispatcherServices.getStreamManagerRunnerFactory();

		this.jobManagerTerminationFutures = new HashMap<>(2);

		this.shutDownFuture = new CompletableFuture<>();

		this.recoveredJobs = new HashSet<>(recoveredJobs);

		dispatcherLeaderRetrievalService = highAvailabilityServices.getDispatcherLeaderRetriever();
		dispatcherGatewayRetriever = new RpcGatewayRetriever<>(
			rpcService,
			DispatcherGateway.class,
			DispatcherId::fromUuid,
			10,
			Time.milliseconds(50L));

	}

	public LeaderGatewayRetriever<DispatcherGateway> getStartedDispatcherRetriever() {
		return this.dispatcherGatewayRetriever;
	}

	//------------------------------------------------------
	// Getters
	//------------------------------------------------------

	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return shutDownFuture;
	}

	//------------------------------------------------------
	// Lifecycle methods
	//------------------------------------------------------

	@Override
	public void onStart() throws Exception {
		try {
			startDispatcherServices();
		} catch (Exception e) {
			final DispatcherException exception = new DispatcherException(String.format("Could not start the Dispatcher %s", getAddress()), e);
			onFatalError(exception);
			throw exception;
		}

		startRecoveredJobs();
	}

	private void startDispatcherServices() throws Exception {
		try {
			// set up the connection in dispatcherGatewayRetriever.
			dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);
		} catch (Exception e) {
			handleStartDispatcherServicesException(e);
		}
		log.info("start dispatcher services");
	}

	private void startRecoveredJobs() {
		log.info("no op currently");
	}

	private BiFunction<Void, Throwable, Void> handleRecoveredJobStartError(JobID jobId) {
		return (ignored, throwable) -> {
			if (throwable != null) {
				onFatalError(new DispatcherException(String.format("Could not start recovered job %s.", jobId), throwable));
			}

			return null;
		};
	}

	private void handleStartDispatcherServicesException(Exception e) throws Exception {
		try {
			stopDispatcherServices();
		} catch (Exception exception) {
			e.addSuppressed(exception);
		}

		throw e;
	}

	@Override
	public CompletableFuture<Void> onStop() {
		log.info("Stopping dispatcher {}.", getAddress());

		final CompletableFuture<Void> allJobManagerRunnersTerminationFuture = terminateJobManagerRunnersAndGetTerminationFuture();

		return FutureUtils.runAfterwards(
			allJobManagerRunnersTerminationFuture,
			() -> {
				stopDispatcherServices();

				log.info("Stopped dispatcher {}.", getAddress());
			});
	}

	private void stopDispatcherServices() throws Exception {
		Exception exception = null;
		try {
			dispatcherLeaderRetrievalService.stop();
			jobManagerSharedServices.shutdown();
		} catch (Exception e) {
			exception = e;
		}

		ExceptionUtils.tryRethrowException(exception);
	}

	//------------------------------------------------------
	// RPCs
	//------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
		log.info("Received JobGraph submission {} ({}).", jobGraph.getJobID(), jobGraph.getName());

		try {
			if (isDuplicateJob(jobGraph.getJobID())) {
				return FutureUtils.completedExceptionally(
					new DuplicateJobSubmissionException(jobGraph.getJobID()));
			} else if (isPartialResourceConfigured(jobGraph)) {
				return FutureUtils.completedExceptionally(
					new JobSubmissionException(jobGraph.getJobID(), "Currently jobs is not supported if parts of the vertices have " +
							"resources configured. The limitation will be removed in future versions."));
			} else {
				// TODO: add create StreamManager logic.
				return internalSubmitJob(jobGraph);
//				return CompletableFuture.completedFuture(Acknowledge.get());
			}
		} catch (FlinkException e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	private CompletableFuture<Acknowledge> internalSubmitJob(JobGraph jobGraph) {
		log.info("[SMD] Submitting job {} ({})", jobGraph.getJobID(), jobGraph.getName());
		// TODO: waitForTerminatingStreamManager
		final CompletableFuture<Acknowledge> submitDirectly = persistAndRunJob(jobGraph)
			.thenApply(ignored -> Acknowledge.get());
		return submitDirectly.handleAsync((acknowledge, throwable) -> {
			if (throwable != null) {
				//TODO: cleanup data

				final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
				log.error("Failed to submit job {}.", jobGraph.getJobID(), strippedThrowable);
				throw new CompletionException(
					new JobSubmissionException(jobGraph.getJobID(), "Failed to submit job.", strippedThrowable));
			} else {
				return acknowledge;
			}
		}, getRpcService().getExecutor());
	}

	private CompletableFuture<Void> persistAndRunJob(JobGraph jobGraph) {
		// TODO: jobGraphWriter
		final CompletableFuture<Void> runSMFuture = runStreamManager(jobGraph);
		return runSMFuture.whenComplete(BiConsumerWithException.unchecked((Object ignored, Throwable throwable) -> {
			if (throwable != null) {
				final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);

				log.error("Failed to run stream manager.", strippedThrowable);
				// jobGraphWriter.removeJobGraph(jobGraph.getJobID());
			}
		}));
	}

	private CompletableFuture<Void> runStreamManager(JobGraph jobGraph) {

		final CompletableFuture<StreamManagerRunner> streamManagerRunnerFuture = createStreamManagerRunner(jobGraph);

		return streamManagerRunnerFuture
			.thenApply(FunctionUtils.uncheckedFunction(this::startStreamManagerRunner))
			.thenApply(FunctionUtils.nullFn())
			.whenCompleteAsync(
				(ignored, throwable) -> {
				},
				getMainThreadExecutor());

	}


	private CompletableFuture<StreamManagerRunner> createStreamManagerRunner(JobGraph jobGraph) {
		final RpcService rpcService = getRpcService();
		return CompletableFuture.supplyAsync(
			CheckedSupplier.unchecked(() ->
				streamManagerRunnerFactory.createStreamManagerRunner(
					jobGraph,
					configuration, //configuration,
					rpcService,
					highAvailabilityServices, //highAvailabilityServices,
					dispatcherGatewayRetriever, //heartbeatServices,
					fatalErrorHandler //fatalErrorHandler
				)),
			rpcService.getExecutor());
	}

	private StreamManagerRunner startStreamManagerRunner(StreamManagerRunner streamManagerRunner) throws Exception {
		streamManagerRunner.start();
		return streamManagerRunner;
	}


	/**
	 * Checks whether the given job has already been submitted or executed.
	 *
	 * @param jobId identifying the submitted job
	 * @return true if the job has already been submitted (is running) or has been executed
	 * @throws FlinkException if the job scheduling status cannot be retrieved
	 */
	private boolean isDuplicateJob(JobID jobId) throws FlinkException {
		final RunningJobsRegistry.JobSchedulingStatus jobSchedulingStatus;

		try {
			jobSchedulingStatus = runningJobsRegistry.getJobSchedulingStatus(jobId);
		} catch (IOException e) {
			throw new FlinkException(String.format("Failed to retrieve job scheduling status for job %s.", jobId), e);
		}

		return jobSchedulingStatus == RunningJobsRegistry.JobSchedulingStatus.DONE || jobManagerRunnerFutures.containsKey(jobId);
	}

	private boolean isPartialResourceConfigured(JobGraph jobGraph) {
		boolean hasVerticesWithUnknownResource = false;
		boolean hasVerticesWithConfiguredResource = false;

		for (JobVertex jobVertex : jobGraph.getVertices()) {
			if (jobVertex.getMinResources() == ResourceSpec.UNKNOWN) {
				hasVerticesWithUnknownResource = true;
			} else {
				hasVerticesWithConfiguredResource = true;
			}

			if (hasVerticesWithUnknownResource && hasVerticesWithConfiguredResource) {
				return true;
			}
		}

		return false;
	}


	@Override
	public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
		return CompletableFuture.completedFuture(
			Collections.unmodifiableSet(new HashSet<>(jobManagerRunnerFutures.keySet())));
	}

	/**
	 * Cleans up the job related data from the dispatcher. If cleanupHA is true, then
	 * the data will also be removed from HA.
	 *
	 * @param jobId JobID identifying the job to clean up
	 * @param cleanupHA True iff HA data shall also be cleaned up
	 */
	private void removeJobAndRegisterTerminationFuture(JobID jobId, boolean cleanupHA) {
		final CompletableFuture<Void> cleanupFuture = removeJob(jobId, cleanupHA);

		registerJobManagerRunnerTerminationFuture(jobId, cleanupFuture);
	}

	private void registerJobManagerRunnerTerminationFuture(JobID jobId, CompletableFuture<Void> jobManagerRunnerTerminationFuture) {
		Preconditions.checkState(!jobManagerTerminationFutures.containsKey(jobId));

		jobManagerTerminationFutures.put(jobId, jobManagerRunnerTerminationFuture);

		// clean up the pending termination future
		jobManagerRunnerTerminationFuture.thenRunAsync(
			() -> {
				final CompletableFuture<Void> terminationFuture = jobManagerTerminationFutures.remove(jobId);

				//noinspection ObjectEquality
				if (terminationFuture != null && terminationFuture != jobManagerRunnerTerminationFuture) {
					jobManagerTerminationFutures.put(jobId, terminationFuture);
				}
			},
			getMainThreadExecutor());
	}

	private CompletableFuture<Void> removeJob(JobID jobId, boolean cleanupHA) {
		CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = jobManagerRunnerFutures.remove(jobId);

		final CompletableFuture<Void> jobManagerRunnerTerminationFuture;
		if (jobManagerRunnerFuture != null) {
			jobManagerRunnerTerminationFuture = jobManagerRunnerFuture.thenCompose(JobManagerRunner::closeAsync);
		} else {
			jobManagerRunnerTerminationFuture = CompletableFuture.completedFuture(null);
		}

		return jobManagerRunnerTerminationFuture.thenRunAsync(
			() -> cleanUpJobData(jobId, cleanupHA),
			getRpcService().getExecutor());
	}

	private void cleanUpJobData(JobID jobId, boolean cleanupHA) {

		boolean cleanupHABlobs = false;
		if (cleanupHA) {
			try {
				jobGraphWriter.removeJobGraph(jobId);

				// only clean up the HA blobs if we could remove the job from HA storage
				cleanupHABlobs = true;
			} catch (Exception e) {
				log.warn("Could not properly remove job {} from submitted job graph store.", jobId, e);
			}

			try {
				runningJobsRegistry.clearJob(jobId);
			} catch (IOException e) {
				log.warn("Could not properly remove job {} from the running jobs registry.", jobId, e);
			}
		} else {
			try {
				jobGraphWriter.releaseJobGraph(jobId);
			} catch (Exception e) {
				log.warn("Could not properly release job {} from submitted job graph store.", jobId, e);
			}
		}

		blobServer.cleanupJob(jobId, cleanupHABlobs);
	}

	/**
	 * Terminate all currently running {@link JobManagerRunnerImpl}.
	 */
	private void terminateJobManagerRunners() {
		log.info("Stopping all currently running jobs of dispatcher {}.", getAddress());

		final HashSet<JobID> jobsToRemove = new HashSet<>(jobManagerRunnerFutures.keySet());

		for (JobID jobId : jobsToRemove) {
			removeJobAndRegisterTerminationFuture(jobId, false);
		}
	}

	private CompletableFuture<Void> terminateJobManagerRunnersAndGetTerminationFuture() {
		terminateJobManagerRunners();
		final Collection<CompletableFuture<Void>> values = jobManagerTerminationFutures.values();
		return FutureUtils.completeAll(values);
	}

	protected void onFatalError(Throwable throwable) {
		fatalErrorHandler.onFatalError(throwable);
	}

	protected void jobNotFinished(JobID jobId) {
		log.info("Job {} was not finished by JobManager.", jobId);

		removeJobAndRegisterTerminationFuture(jobId, false);
	}

	private void jobMasterFailed(JobID jobId, Throwable cause) {
		// we fail fatally in case of a JobMaster failure in order to restart the
		// dispatcher to recover the jobs again. This only works in HA mode, though
		onFatalError(new FlinkException(String.format("JobMaster for job %s failed.", jobId), cause));
	}

	private CompletableFuture<JobMasterGateway> getJobMasterGatewayFuture(JobID jobId) {
		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = jobManagerRunnerFutures.get(jobId);

		if (jobManagerRunnerFuture == null) {
			return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
		} else {
			final CompletableFuture<JobMasterGateway> leaderGatewayFuture = jobManagerRunnerFuture.thenCompose(JobManagerRunner::getJobMasterGateway);
			return leaderGatewayFuture.thenApplyAsync(
				(JobMasterGateway jobMasterGateway) -> {
					// check whether the retrieved JobMasterGateway belongs still to a running JobMaster
					if (jobManagerRunnerFutures.containsKey(jobId)) {
						return jobMasterGateway;
					} else {
						throw new CompletionException(new FlinkJobNotFoundException(jobId));
					}
				},
				getMainThreadExecutor());
		}
	}


	public CompletableFuture<Void> onRemovedJobGraph(JobID jobId) {
		return CompletableFuture.runAsync(
			() -> removeJobAndRegisterTerminationFuture(jobId, false),
			getMainThreadExecutor());
	}

	@Override
	public CompletableFuture<Integer> getBlobServerPort(Time timeout) {
		return CompletableFuture.completedFuture(blobServer.getPort());
	}
}
