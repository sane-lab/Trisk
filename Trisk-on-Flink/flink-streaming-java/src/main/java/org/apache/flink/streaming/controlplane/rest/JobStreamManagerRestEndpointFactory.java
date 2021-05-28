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

package org.apache.flink.streaming.controlplane.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.streaming.controlplane.dispatcher.StreamManagerDispatcherGateway;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerMiniDispatcherRestEndpoint;
import org.apache.flink.streaming.controlplane.webmonitor.StreamManagerRestfulGateway;
import org.apache.flink.streaming.controlplane.webmonitor.StreamManagerWebMonitorEndpoint;
import org.apache.flink.runtime.jobmaster.MiniDispatcherRestEndpoint;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rest.RestEndpointFactory;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

import java.util.concurrent.ScheduledExecutorService;

/**
 * {@link RestEndpointFactory} which creates a {@link MiniDispatcherRestEndpoint}.
 */
public enum JobStreamManagerRestEndpointFactory implements StreamManagerRestEndpointFactory<StreamManagerRestfulGateway> {
	INSTANCE;

	@Override
	public StreamManagerWebMonitorEndpoint<StreamManagerRestfulGateway> createRestEndpoint(
			Configuration configuration,
			LeaderGatewayRetriever<StreamManagerDispatcherGateway> dispatcherGatewayRetriever,
			TransientBlobService transientBlobService,
			ScheduledExecutorService executor,
			LeaderElectionService leaderElectionService,
			FatalErrorHandler fatalErrorHandler) throws Exception {
		final RestHandlerConfiguration restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(configuration);

		return new StreamManagerMiniDispatcherRestEndpoint(
			RestServerEndpointConfiguration.fromConfigurationForSm(configuration),
			dispatcherGatewayRetriever,
			configuration,
			restHandlerConfiguration,
			executor,
			leaderElectionService,
			RestEndpointFactory.createExecutionGraphCache(restHandlerConfiguration),
			fatalErrorHandler);
	}
}
