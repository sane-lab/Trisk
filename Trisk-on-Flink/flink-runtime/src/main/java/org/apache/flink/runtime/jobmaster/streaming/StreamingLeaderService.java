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

package org.apache.flink.runtime.jobmaster.streaming;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerGateway;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerId;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * This service has the responsibility to monitor the job leaders (the job manager which is leader
 * for a given job) for all registered jobs. Upon gaining leadership for a job and detection by the
 * job leader service, the service tries to establish a connection to the job leader. After
 * successfully establishing a connection, the job leader listener is notified about the new job
 * leader and its connection. In case that a job leader loses leadership, the job leader listener
 * is notified as well.
 */
public class StreamingLeaderService {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingLeaderService.class);

	/**
	 * The leader retrieval service and listener for each registered job.
	 */
	private final Map<JobID, Tuple2<LeaderRetrievalService, StreamManagerLeaderListener>> streamManagerLeaderServices;

	private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

	/**
	 * Internal state of the service.
	 */
	private volatile StreamingLeaderService.State state;

	/**
	 * Address of the owner of this service. This address is used for the job manager connection.
	 */
	private JobMasterLocation jobMasterLocation;

	/**
	 * Rpc service to use for establishing connections.
	 */
	private RpcService rpcService;

	/**
	 * High availability services to create the leader retrieval services from.
	 */
	private HighAvailabilityServices highAvailabilityServices;

	/**
	 * Job leader listener listening for job leader changes.
	 */
	private StreamingLeaderListener streamManagerLeaderListener;

	public StreamingLeaderService(
		RetryingRegistrationConfiguration retryingRegistrationConfiguration) {
		this.retryingRegistrationConfiguration = Preconditions.checkNotNull(retryingRegistrationConfiguration);

		// Has to be a concurrent hash map because tests might access this service
		// concurrently via containsJob
		streamManagerLeaderServices = new ConcurrentHashMap<>(4);

		state = StreamingLeaderService.State.CREATED;

		jobMasterLocation = null;
		rpcService = null;
		highAvailabilityServices = null;
		streamManagerLeaderListener = null;
	}

	// -------------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------------

	/**
	 * todo this method only connect the corresponding stream manager
	 * Start the stream manager leader service with the given services.
	 *
	 * @param initialJobMasterLocation             to be used for establishing connections (source address)
	 * @param initialRpcService               to be used to create rpc connections
	 * @param initialHighAvailabilityServices to create leader retrieval services for the different jobs
	 * @param initialStreamingLeaderListener  listening for stream leader changes
	 */
	public void start(
		final JobMasterLocation initialJobMasterLocation,
		final RpcService initialRpcService,
		final HighAvailabilityServices initialHighAvailabilityServices,
		final StreamingLeaderListener initialStreamingLeaderListener) throws Exception {

		if (StreamingLeaderService.State.CREATED != state) {
			throw new IllegalStateException("The service has already been started.");
		} else {
			LOG.info("Start job leader service.");

			this.jobMasterLocation = Preconditions.checkNotNull(initialJobMasterLocation);
			this.rpcService = Preconditions.checkNotNull(initialRpcService);
			this.highAvailabilityServices = Preconditions.checkNotNull(initialHighAvailabilityServices);
			this.streamManagerLeaderListener = Preconditions.checkNotNull(initialStreamingLeaderListener);
			state = StreamingLeaderService.State.STARTED;
		}
	}


	/**
	 * Stop the job leader services. This implies stopping all leader retrieval services for the
	 * different jobs and their leader retrieval listeners.
	 *
	 * @throws Exception if an error occurs while stopping the service
	 */
	public void stop() throws Exception {
		LOG.info("Stop job leader service.");

		if (StreamingLeaderService.State.STARTED == state) {

			for (Tuple2<LeaderRetrievalService, StreamManagerLeaderListener> leaderRetrievalServiceEntry : streamManagerLeaderServices.values()) {
				LeaderRetrievalService leaderRetrievalService = leaderRetrievalServiceEntry.f0;
				StreamManagerLeaderListener streamManagerLeaderListener = leaderRetrievalServiceEntry.f1;

				streamManagerLeaderListener.stop();
				leaderRetrievalService.stop();
			}

			streamManagerLeaderServices.clear();
		}

		state = StreamingLeaderService.State.STOPPED;
	}

	/**
	 * Remove the given job from being monitored by the job leader service.
	 *
	 * @param jobId identifying the job to remove from monitoring
	 * @throws Exception if an error occurred while stopping the leader retrieval service and listener
	 */
	public void removeJob(JobID jobId) throws Exception {
		Preconditions.checkState(StreamingLeaderService.State.STARTED == state, "The service is currently not running.");

		Tuple2<LeaderRetrievalService, StreamManagerLeaderListener> entry = streamManagerLeaderServices.remove(jobId);

		if (entry != null) {
			LOG.info("Remove job {} from job leader monitoring.", jobId);

			LeaderRetrievalService leaderRetrievalService = entry.f0;
			StreamManagerLeaderListener streamManagerLeaderListener = entry.f1;

			leaderRetrievalService.stop();
			streamManagerLeaderListener.stop();
		}
	}

	/**
	 * Add the given job to be monitored. This means that the service tries to detect leaders for
	 * this job and then tries to establish a connection to it.
	 *
	 * @param jobId                identifying the job to monitor
	 * @param defaultTargetAddress of the stream manager leader
	 * @throws Exception if an error occurs while starting the leader retrieval service
	 */
	public void addJob(final JobID jobId, final String defaultTargetAddress) throws Exception {
		Preconditions.checkState(StreamingLeaderService.State.STARTED == state, "The service is currently not running.");

		LOG.info("Add job {} for job leader monitoring.", jobId);

		final LeaderRetrievalService leaderRetrievalService = highAvailabilityServices.getStreamManagerLeaderRetriever(
			jobId,
			defaultTargetAddress);

		StreamManagerLeaderListener streamManagerLeaderListener = new StreamManagerLeaderListener(jobId);

		final Tuple2<LeaderRetrievalService, StreamManagerLeaderListener> oldEntry = streamManagerLeaderServices.put(jobId, Tuple2.of(leaderRetrievalService, streamManagerLeaderListener));

		if (oldEntry != null) {
			oldEntry.f0.stop();
			oldEntry.f1.stop();
		}

		leaderRetrievalService.start(streamManagerLeaderListener);
	}

	/**
	 * Triggers reconnection to the last known leader of the given job.
	 *
	 * @param jobId specifying the job for which to trigger reconnection
	 */
	public void reconnect(final JobID jobId) {
		Preconditions.checkNotNull(jobId, "JobID must not be null.");

		final Tuple2<LeaderRetrievalService, StreamManagerLeaderListener> jobLeaderService = streamManagerLeaderServices.get(jobId);

		if (jobLeaderService != null) {
			jobLeaderService.f1.reconnect();
		} else {
			LOG.info("Cannot reconnect to job {} because it is not registered.", jobId);
		}
	}

	/**
	 * Leader listener which tries to establish a connection to a newly detected stream manager leader.
	 */
	@ThreadSafe
	private final class StreamManagerLeaderListener implements LeaderRetrievalListener {

		private final Object lock = new Object();

		/**
		 * Job id identifying the job to look for a leader.
		 */
		private final JobID jobId;

		/**
		 * Rpc connection to the job leader.
		 */
		@GuardedBy("lock")
		@Nullable
		private RegisteredRpcConnection<StreamManagerId, StreamManagerGateway, JMTMRegistrationSuccess> rpcConnection;

		/**
		 * Leader id of the current job leader.
		 */
		@GuardedBy("lock")
		@Nullable
		private StreamManagerId currentStreamManagerId;

		/**
		 * State of the listener.
		 */
		private volatile boolean stopped;

		private StreamManagerLeaderListener(JobID jobId) {
			this.jobId = Preconditions.checkNotNull(jobId);

			stopped = false;
			rpcConnection = null;
			currentStreamManagerId = null;
		}

		private StreamManagerId getCurrentStreamManagerId() {
			synchronized (lock) {
				return currentStreamManagerId;
			}
		}

		public void stop() {
			synchronized (lock) {
				if (!stopped) {
					stopped = true;

					closeRpcConnection();
				}
			}
		}

		public void reconnect() {
			synchronized (lock) {
				if (stopped) {
					LOG.debug("Cannot reconnect because the JobManagerLeaderListener has already been stopped.");
				} else {
					if (rpcConnection != null) {
						Preconditions.checkState(
							rpcConnection.tryReconnect(),
							"Illegal concurrent modification of the JobManagerLeaderListener rpc connection.");
					} else {
						LOG.debug("Cannot reconnect to an unknown JobMaster.");
					}
				}
			}
		}

		@Override
		public void notifyLeaderAddress(final @Nullable String leaderAddress, final @Nullable UUID leaderId) {
			Optional<StreamManagerId> streamManagerLostLeadership = Optional.empty();

			synchronized (lock) {
				if (stopped) {
					LOG.debug("{}'s leader retrieval listener reported a new leader for job {}. " +
						"However, the service is no longer running.", StreamingLeaderService.class.getSimpleName(), jobId);
				} else {
					final StreamManagerId streamManagerId = StreamManagerId.fromUuidOrNull(leaderId);

					LOG.debug("New leader information for job {}. Address: {}, leader id: {}.",
						jobId, leaderAddress, streamManagerId);

					if (leaderAddress == null || leaderAddress.isEmpty()) {
						// the leader lost leadership but there is no other leader yet.
						streamManagerLostLeadership = Optional.ofNullable(currentStreamManagerId);
						closeRpcConnection();
					} else {
						// check whether we are already connecting to this leader
						if (Objects.equals(streamManagerId, currentStreamManagerId)) {
							LOG.debug("Ongoing attempt to connect to leader of job {}. Ignoring duplicate leader information.", jobId);
						} else {
							closeRpcConnection();
							openRpcConnectionTo(leaderAddress, streamManagerId);
						}
					}
				}
			}

			// send callbacks outside of the lock scope
			streamManagerLostLeadership.ifPresent(
				oldStreamManagerId -> streamManagerLeaderListener.streamManagerLostLeadership(jobId, oldStreamManagerId)
			);
		}

		@GuardedBy("lock")
		private void openRpcConnectionTo(String leaderAddress, StreamManagerId streamManagerId) {
			Preconditions.checkState(
				currentStreamManagerId == null && rpcConnection == null,
				"Cannot open a new rpc connection if the previous connection has not been closed.");

			currentStreamManagerId = streamManagerId;
			rpcConnection = new StreamManagerRegisteredRpcConnection(
				LOG,
				leaderAddress,
				streamManagerId,
				jobMasterLocation,
				rpcService.getExecutor());

			LOG.info("Try to register at job manager {} with leader id {}.", leaderAddress, streamManagerId.toUUID());
			rpcConnection.start();
		}

		@GuardedBy("lock")
		private void closeRpcConnection() {
			if (rpcConnection != null) {
				rpcConnection.close();
				rpcConnection = null;
				currentStreamManagerId = null;
			}
		}

		@Override
		public void handleError(Exception exception) {
			if (stopped) {
				LOG.debug("{}'s leader retrieval listener reported an exception for job {}. " +
						"However, the service is no longer running.", StreamingLeaderService.class.getSimpleName(),
					jobId, exception);
			} else {
				streamManagerLeaderListener.handleError(exception);
			}
		}

		/**
		 * Rpc connection for the job manager <--> task manager connection.
		 */
		private final class StreamManagerRegisteredRpcConnection extends RegisteredRpcConnection<StreamManagerId, StreamManagerGateway, JMTMRegistrationSuccess> {
			private final JobMasterLocation jobMasterLocation;

			StreamManagerRegisteredRpcConnection(
				Logger log,
				String targetAddress,
				StreamManagerId streamManagerId,
				JobMasterLocation jobMasterLocation,
				Executor executor) {
				super(log, targetAddress, streamManagerId, executor);
				this.jobMasterLocation = jobMasterLocation;
			}

			@Override
			protected RetryingRegistration<StreamManagerId, StreamManagerGateway, JMTMRegistrationSuccess> generateRegistration() {
				return new StreamManagerRetryingRegistration(
					LOG,
					rpcService,
					"StreamManager",
					StreamManagerGateway.class,
					getTargetAddress(),
					getTargetLeaderId(),
					retryingRegistrationConfiguration,
					jobMasterLocation);
			}

			@Override
			protected void onRegistrationSuccess(JMTMRegistrationSuccess success) {
				// filter out old registration attempts
				if (Objects.equals(getTargetLeaderId(), getCurrentStreamManagerId())) {
					log.info("Successful registration at job manager {} for job {}.", getTargetAddress(), jobId);

					streamManagerLeaderListener.streamManagerGainedLeadership(jobId, getTargetGateway(), success);
				} else {
					log.debug("Encountered obsolete JobManager registration success from {} with leader session ID {}.", getTargetAddress(), getTargetLeaderId());
				}
			}

			@Override
			protected void onRegistrationFailure(Throwable failure) {
				// filter out old registration attempts
				if (Objects.equals(getTargetLeaderId(), getCurrentStreamManagerId())) {
					log.info("Failed to register at job  manager {} for job {}.", getTargetAddress(), jobId);
					streamManagerLeaderListener.handleError(failure);
				} else {
					log.debug("Obsolete JobManager registration failure from {} with leader session ID {}.", getTargetAddress(), getTargetLeaderId(), failure);
				}
			}
		}
	}

	/**
	 * Retrying registration for the job manager <--> task manager connection.
	 */
	private static final class StreamManagerRetryingRegistration
		extends RetryingRegistration<StreamManagerId, StreamManagerGateway, JMTMRegistrationSuccess> {

		private final JobMasterLocation jobMasterLocation;

		StreamManagerRetryingRegistration(
			Logger log,
			RpcService rpcService,
			String targetName,
			Class<StreamManagerGateway> targetType,
			String targetAddress,
			StreamManagerId streamManagerId,
			RetryingRegistrationConfiguration retryingRegistrationConfiguration,
			JobMasterLocation jobMasterLocation) {
			super(
				log,
				rpcService,
				targetName,
				targetType,
				targetAddress,
				streamManagerId,
				retryingRegistrationConfiguration);

			this.jobMasterLocation = jobMasterLocation;
		}

		@Override
		protected CompletableFuture<RegistrationResponse> invokeRegistration(
			StreamManagerGateway gateway,
			StreamManagerId streamManagerId,
			long timeoutMillis) {
			return gateway.registerJobManager(
				jobMasterLocation.jobMasterId,
				jobMasterLocation.jobManagerResourceID,
				jobMasterLocation.jobMasterRpcAddress,
				jobMasterLocation.jobID,
				Time.milliseconds(timeoutMillis)
			);
		}
	}

	public static class JobMasterLocation {
		final JobMasterId jobMasterId;
		final ResourceID jobManagerResourceID;
		final String jobMasterRpcAddress;
		final JobID jobID;

		public JobMasterLocation(JobMasterId jobMasterId, ResourceID jobManagerResourceID, String jobMasterRpcAddress, JobID jobID) {
			this.jobMasterId = jobMasterId;
			this.jobManagerResourceID = jobManagerResourceID;
			this.jobMasterRpcAddress = jobMasterRpcAddress;
			this.jobID = jobID;
		}
	}

	/**
	 * Internal state of the service.
	 */
	private enum State {
		CREATED, STARTED, STOPPED
	}

	// -----------------------------------------------------------
	// Testing methods
	// -----------------------------------------------------------

	/**
	 * Check whether the service monitors the given job.
	 *
	 * @param jobId identifying the job
	 * @return True if the given job is monitored; otherwise false
	 */
	@VisibleForTesting
	public boolean containsJob(JobID jobId) {
		Preconditions.checkState(StreamingLeaderService.State.STARTED == state, "The service is currently not running.");

		return streamManagerLeaderServices.containsKey(jobId);
	}
}
