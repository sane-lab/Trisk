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

package org.apache.flink.streaming.controlplane.entrypoint.component;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.streaming.controlplane.dispatcher.PartialStreamManagerDispatcherServices;
import org.apache.flink.streaming.controlplane.dispatcher.SessionStreamManagerDispatcherFactory;
import org.apache.flink.streaming.controlplane.dispatcher.StreamManagerDispatcherGateway;
import org.apache.flink.streaming.controlplane.dispatcher.StreamManagerDispatcherId;
import org.apache.flink.streaming.controlplane.dispatcher.runner.DefaultStreamManagerDispatcherRunnerFactory;
import org.apache.flink.streaming.controlplane.dispatcher.runner.StreamManagerDispatcherRunner;
import org.apache.flink.streaming.controlplane.dispatcher.runner.StreamManagerDispatcherRunnerFactory;
import org.apache.flink.streaming.controlplane.rest.JobStreamManagerRestEndpointFactory;
import org.apache.flink.streaming.controlplane.rest.SessionStreamManagerRestEndpointFactory;
import org.apache.flink.streaming.controlplane.rest.StreamManagerRestEndpointFactory;
import org.apache.flink.streaming.controlplane.webmonitor.StreamManagerWebMonitorEndpoint;
import org.apache.flink.runtime.dispatcher.*;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.JobGraphRetriever;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmanager.HaServicesJobGraphStoreFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract class which implements the creation of the {@link DispatcherResourceManagerComponent} components.
 */
public class DefaultStreamManagerDispatcherComponentFactory implements StreamManagerDispatcherComponentFactory {

	private final Logger log = LoggerFactory.getLogger(getClass());

	@Nonnull
	private final StreamManagerRestEndpointFactory<?> smRestEndpointFactory;

	@Nonnull
	private final StreamManagerDispatcherRunnerFactory smDispatcherRunnerFactory;

	DefaultStreamManagerDispatcherComponentFactory(
		@Nonnull StreamManagerDispatcherRunnerFactory smDispatcherRunnerFactory,
		@Nonnull StreamManagerRestEndpointFactory<?> smRestEndpointFactory) {
		this.smDispatcherRunnerFactory = smDispatcherRunnerFactory;
		this.smRestEndpointFactory = smRestEndpointFactory;
	}

	@Override
	public StreamManagerDispatcherComponent create(
		Configuration configuration,
		Executor ioExecutor,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		BlobServer blobServer,
		HeartbeatServices heartbeatServices,
		FatalErrorHandler fatalErrorHandler) throws Exception {

		LeaderRetrievalService smDispatcherLeaderRetrievalService = null;
		StreamManagerWebMonitorEndpoint<?> smWebMonitorEndpoint = null;
		StreamManagerDispatcherRunner smDispatcherRunner = null;

		try {
			smDispatcherLeaderRetrievalService = highAvailabilityServices.getStreamManagerDispatcherLeaderRetriever();

			final LeaderGatewayRetriever<StreamManagerDispatcherGateway> smDispatcherGatewayRetriever = new RpcGatewayRetriever<>(
				rpcService,
				StreamManagerDispatcherGateway.class,
				StreamManagerDispatcherId::fromUuid,
				10,
				Time.milliseconds(50L));

			final ScheduledExecutorService executor = StreamManagerWebMonitorEndpoint.createExecutorService(
				configuration.getInteger(RestOptions.SERVER_NUM_THREADS),
				configuration.getInteger(RestOptions.SERVER_THREAD_PRIORITY),
				"DispatcherRestEndpoint");

			smWebMonitorEndpoint = smRestEndpointFactory.createRestEndpoint(
				configuration,
				smDispatcherGatewayRetriever,
				blobServer,
				executor,
				highAvailabilityServices.getClusterRestEndpointLeaderElectionService(),
				fatalErrorHandler);

			log.debug("Starting StreamManagerDispatcher REST endpoint.");
			smWebMonitorEndpoint.start();

			final PartialStreamManagerDispatcherServices partialSmDispatcherServices = new PartialStreamManagerDispatcherServices(
				configuration,
				highAvailabilityServices,
				blobServer,
				heartbeatServices,
				fatalErrorHandler);

			log.debug("Starting sm Dispatcher.");
			smDispatcherRunner = smDispatcherRunnerFactory.createStreamManagerDispatcherRunner(
				highAvailabilityServices.getStreamManagerDispatcherLeaderElectionService(),
				fatalErrorHandler,
				new HaServicesJobGraphStoreFactory(highAvailabilityServices),
				ioExecutor,
				rpcService,
				partialSmDispatcherServices
			);


			smDispatcherLeaderRetrievalService.start(smDispatcherGatewayRetriever);

			return new StreamManagerDispatcherComponent(
				smDispatcherRunner,
				smDispatcherLeaderRetrievalService,
				smWebMonitorEndpoint
				);

		} catch (Exception exception) {
			// clean up all started components
			if (smDispatcherLeaderRetrievalService != null) {
				try {
					smDispatcherLeaderRetrievalService.stop();
				} catch (Exception e) {
					exception = ExceptionUtils.firstOrSuppressed(e, exception);
				}
			}

			final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);


			if (smDispatcherRunner != null) {
				terminationFutures.add(smDispatcherRunner.closeAsync());
			}

			final FutureUtils.ConjunctFuture<Void> terminationFuture = FutureUtils.completeAll(terminationFutures);

			try {
				terminationFuture.get();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			throw new FlinkException("Could not create the DispatcherResourceManagerComponent.", exception);
		}
	}

	public static DefaultStreamManagerDispatcherComponentFactory createSessionComponentFactory() {
		return new DefaultStreamManagerDispatcherComponentFactory(
			DefaultStreamManagerDispatcherRunnerFactory.createSessionRunner(SessionStreamManagerDispatcherFactory.INSTANCE),
			SessionStreamManagerRestEndpointFactory.INSTANCE);
	}

	public static DefaultStreamManagerDispatcherComponentFactory createJobComponentFactory(
		JobGraphRetriever jobGraphRetriever) {
		return new DefaultStreamManagerDispatcherComponentFactory(
			DefaultStreamManagerDispatcherRunnerFactory.createJobRunner(jobGraphRetriever),
			JobStreamManagerRestEndpointFactory.INSTANCE);
	}
}
