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

package org.apache.flink.streaming.controlplane.streammanager.factories;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.streaming.controlplane.streammanager.StreamManager;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerConfiguration;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerRuntimeServices;

/**
 * Default implementation of the {@link StreamManagerServiceFactory}.
 * TODO:
 * 1. decide about the StreamManagerSharedServices
 */
public class DefaultStreamManagerServiceFactory implements StreamManagerServiceFactory {

	private final StreamManagerConfiguration streamManagerConfiguration;

	private final RpcService rpcService;

	private final HighAvailabilityServices haServices;

//	private final JobManagerSharedServices jobManagerSharedServices;
	// TODO: May need StreamManagerSharedServices


	private final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever;

	private final FatalErrorHandler fatalErrorHandler;

	public DefaultStreamManagerServiceFactory(
		StreamManagerConfiguration streamManagerConfiguration,
		RpcService rpcService,
		HighAvailabilityServices haServices,
		LibraryCacheManager libraryCacheManager,
		LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
		FatalErrorHandler fatalErrorHandler) {
		this.streamManagerConfiguration = streamManagerConfiguration;
		this.rpcService = rpcService;
		this.haServices = haServices;
		this.dispatcherGatewayRetriever = dispatcherGatewayRetriever;
		this.fatalErrorHandler = fatalErrorHandler;
	}

	@Override
	public StreamManager createStreamManagerService(
		JobGraph jobGraph, ClassLoader userCodeLoader) throws Exception {
		final StreamManagerRuntimeServices streamManagerRuntimeServices = StreamManagerRuntimeServices.fromConfiguration(
				streamManagerConfiguration,
				haServices,
				rpcService.getScheduledExecutor());

		return new StreamManager(
			rpcService,
			streamManagerConfiguration,
			ResourceID.generate(),
			jobGraph,
			userCodeLoader,
			haServices,
			streamManagerRuntimeServices.getJobLeaderIdService(),
			dispatcherGatewayRetriever,
			fatalErrorHandler);
	}
}
