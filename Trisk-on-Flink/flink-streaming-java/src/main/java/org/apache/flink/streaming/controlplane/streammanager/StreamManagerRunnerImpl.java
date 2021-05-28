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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerGateway;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerId;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.jobmaster.JobNotFinishedException;
import org.apache.flink.streaming.controlplane.streammanager.factories.StreamManagerServiceFactory;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.FunctionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author trx
 * The runner for the stream manager.
 */
public class StreamManagerRunnerImpl implements LeaderContender, StreamManagerRunner {

	private static final Logger log = LoggerFactory.getLogger(StreamManagerRunnerImpl.class);

	// ------------------------------------------------------------------------

	/** Lock to ensure that this runner can deal with leader election event and notifies simultaneously. */
	private final Object lock = new Object();

	/** The job graph needs to run. */
	private final JobGraph jobGraph;

//	/** Used to check whether a job needs to be run. */
//	private final RunningJobsRegistry runningJobsRegistry;

	/** Leader election for this stream manger. */
	private final LeaderElectionService leaderElectionService;

	private final LibraryCacheManager libraryCacheManager;

	private final Executor executor;

	private final StreamManagerService streamManagerService;

	private final FatalErrorHandler fatalErrorHandler;

	private final CompletableFuture<ArchivedExecutionGraph> resultFuture;

	private final CompletableFuture<Void> terminationFuture;

	private CompletableFuture<Void> leadershipOperation;

	/** flag marking the runner as shut down. */
	private volatile boolean shutdown;

	private volatile CompletableFuture<StreamManagerGateway> leaderGatewayFuture;

	// ------------------------------------------------------------------------

	/**
	 * Exceptions that occur while creating the JobManager or JobManagerRunnerImpl are directly
	 * thrown and not reported to the given {@code FatalErrorHandler}.
	 *
	 * @throws Exception Thrown if the runner cannot be set up, because either one of the
	 *                   required services could not be started, or the Job could not be initialized.
	 */
	public StreamManagerRunnerImpl(
		final JobGraph jobGraph,
		final StreamManagerServiceFactory streamManagerFactory,
		final HighAvailabilityServices haServices,
		final LibraryCacheManager libraryCacheManager,
		final Executor executor,
		final FatalErrorHandler fatalErrorHandler) throws Exception {

		this.resultFuture = new CompletableFuture<>();
		this.terminationFuture = new CompletableFuture<>();
		this.leadershipOperation = CompletableFuture.completedFuture(null);

		// make sure we cleanly shut down out JobManager services if initialization fails
		try {
			this.jobGraph = checkNotNull(jobGraph);
			this.executor = checkNotNull(executor);
			this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
			this.libraryCacheManager = checkNotNull(libraryCacheManager);

			checkArgument(jobGraph.getNumberOfVertices() > 0, "The given job is empty");

			// libraries and class loader first
			try {
				libraryCacheManager.registerJob(
					jobGraph.getJobID(), jobGraph.getUserJarBlobKeys(), jobGraph.getClasspaths());
			} catch (IOException e) {
				throw new Exception("Cannot set up the user code libraries: " + e.getMessage(), e);
			}

			final ClassLoader userCodeLoader = libraryCacheManager.getClassLoader(jobGraph.getJobID());
			if (userCodeLoader == null) {
				throw new Exception("The user code class loader could not be initialized.");
			}

			// high availability services next
			this.leaderElectionService = haServices.getJobManagerLeaderElectionService(jobGraph.getJobID());

			this.leaderGatewayFuture = new CompletableFuture<>();

			// now start the StreamManager
			this.streamManagerService = streamManagerFactory.createStreamManagerService(jobGraph, userCodeLoader);
		}
		catch (Throwable t) {
			terminationFuture.completeExceptionally(t);
			resultFuture.completeExceptionally(t);

			throw new JobExecutionException(jobGraph.getJobID(), "Could not set up StreamManager", t);
		}
	}

	//----------------------------------------------------------------------------------------------
	// Getter
	//----------------------------------------------------------------------------------------------

	@Override
	public CompletableFuture<StreamManagerGateway> getStreamManagerGateway() {
		return leaderGatewayFuture;
	}

	@Override
	public JobID getJobID() {
		return jobGraph.getJobID();
	}

	//----------------------------------------------------------------------------------------------
	// Lifecycle management
	//----------------------------------------------------------------------------------------------

	@Override
	public void start() throws Exception {
		try {
			leaderElectionService.start(this);
		} catch (Exception e) {
			log.error("Could not start the JobManager because the leader election service did not start.", e);
			throw new Exception("Could not start the leader election service.", e);
		}
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		synchronized (lock) {
			if (!shutdown) {
				shutdown = true;

				setNewLeaderGatewayFuture();
				leaderGatewayFuture.completeExceptionally(new FlinkException("JobMaster has been shut down."));

				final CompletableFuture<Void> jobManagerTerminationFuture = streamManagerService.closeAsync();

				jobManagerTerminationFuture.whenComplete(
					(Void ignored, Throwable throwable) -> {
						try {
							leaderElectionService.stop();
						} catch (Throwable t) {
							throwable = ExceptionUtils.firstOrSuppressed(t, ExceptionUtils.stripCompletionException(throwable));
						}

						libraryCacheManager.unregisterJob(jobGraph.getJobID());

						if (throwable != null) {
							terminationFuture.completeExceptionally(
								new FlinkException("Could not properly shut down the JobManagerRunner", throwable));
						} else {
							terminationFuture.complete(null);
						}
					});

				terminationFuture.whenComplete(
					(Void ignored, Throwable throwable) -> {
						resultFuture.completeExceptionally(new JobNotFinishedException(jobGraph.getJobID()));
					});
			}

			return terminationFuture;
		}
	}

	//----------------------------------------------------------------------------------------------
	// Result and error handling methods
	//----------------------------------------------------------------------------------------------

	private void handleJobManagerRunnerError(Throwable cause) {
		if (ExceptionUtils.isJvmFatalError(cause)) {
			fatalErrorHandler.onFatalError(cause);
		} else {
			resultFuture.completeExceptionally(cause);
		}
	}

	//----------------------------------------------------------------------------------------------
	// Leadership methods
	//----------------------------------------------------------------------------------------------

	@Override
	public void grantLeadership(final UUID leaderSessionID) {
		synchronized (lock) {
			if (shutdown) {
				log.info("JobManagerRunner already shutdown.");
				return;
			}

			leadershipOperation = leadershipOperation.thenCompose(
				(ignored) -> {
					synchronized (lock) {
//						return verifySomethingAndStartStreamManager(leaderSessionID);
						return startStreamManager(leaderSessionID);
					}
				});

			handleException(leadershipOperation, "Could not start the stream manager.");
		}
	}

	private CompletableFuture<Void> verifySomethingAndStartStreamManager(UUID leaderSessionId) {
//		final CompletableFuture<JobSchedulingStatus> jobSchedulingStatusFuture = getJobSchedulingStatus();
//		return jobSchedulingStatusFuture.thenCompose(
//			jobSchedulingStatus -> {
//				if (jobSchedulingStatus == JobSchedulingStatus.DONE) {
//					return jobAlreadyDone();
//				} else {
//					return startJobMaster(leaderSessionId);
//				}
//			});
		return null;
	}

	private CompletionStage<Void> startStreamManager(UUID leaderSessionId) {
		log.info("StreamManager runner for job {} ({}) was granted leadership with session id {} at {}.",
			jobGraph.getName(), jobGraph.getJobID(), leaderSessionId, streamManagerService.getAddress());

		final CompletableFuture<Acknowledge> startFuture;
		try {
			startFuture = streamManagerService.start(new StreamManagerId(leaderSessionId));
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(new FlinkException("Failed to start the StreamManager.", e));
		}

		final CompletableFuture<StreamManagerGateway> currentLeaderGatewayFuture = leaderGatewayFuture;
		return startFuture.thenAcceptAsync(
			(Acknowledge ack) -> confirmLeaderSessionIdIfStillLeader(
				leaderSessionId,
				streamManagerService.getAddress(),
				currentLeaderGatewayFuture),
			executor);
	}

	private void confirmLeaderSessionIdIfStillLeader(
			UUID leaderSessionId,
			String leaderAddress,
			CompletableFuture<StreamManagerGateway> currentLeaderGatewayFuture) {

		if (leaderElectionService.hasLeadership(leaderSessionId)) {
			currentLeaderGatewayFuture.complete(streamManagerService.getGateway());
			leaderElectionService.confirmLeadership(leaderSessionId, leaderAddress);
		} else {
			log.debug("Ignoring confirmation of leader session id because {} is no longer the leader.", getDescription());
		}
	}

	@Override
	public void revokeLeadership() {
		synchronized (lock) {
			if (shutdown) {
				log.info("JobManagerRunner already shutdown.");
				return;
			}

			leadershipOperation = leadershipOperation.thenCompose(
				(ignored) -> {
					synchronized (lock) {
						return revokeJobMasterLeadership();
					}
				});

			handleException(leadershipOperation, "Could not suspend the job manager.");
		}
	}

	private CompletableFuture<Void> revokeJobMasterLeadership() {
		log.info("JobManager for job {} ({}) at {} was revoked leadership.",
			jobGraph.getName(), jobGraph.getJobID(), streamManagerService.getAddress());

		setNewLeaderGatewayFuture();

		return streamManagerService
			.suspend(new FlinkException("JobManager is no longer the leader."))
			.thenApply(FunctionUtils.nullFn());
	}

	private void handleException(CompletableFuture<Void> leadershipOperation, String message) {
		leadershipOperation.whenComplete(
			(ignored, throwable) -> {
				if (throwable != null) {
					handleJobManagerRunnerError(new FlinkException(message, throwable));
				}
			});
	}

	private void setNewLeaderGatewayFuture() {
		final CompletableFuture<StreamManagerGateway> oldLeaderGatewayFuture = leaderGatewayFuture;

		leaderGatewayFuture = new CompletableFuture<>();

		if (!oldLeaderGatewayFuture.isDone()) {
			leaderGatewayFuture.whenComplete(
				(StreamManagerGateway streamManagerGateway, Throwable throwable) -> {
					if (throwable != null) {
						oldLeaderGatewayFuture.completeExceptionally(throwable);
					} else {
						oldLeaderGatewayFuture.complete(streamManagerGateway);
					}
				});
		}
	}

	@Override
	public String getDescription() {
		return streamManagerService.getAddress();
	}

	@Override
	public void handleError(Exception exception) {
		log.error("Leader Election Service encountered a fatal error.", exception);
		handleJobManagerRunnerError(exception);
	}

	//----------------------------------------------------------------------------------------------
	// Testing
	//----------------------------------------------------------------------------------------------

	@VisibleForTesting
	boolean isShutdown() {
		return shutdown;
	}
}
