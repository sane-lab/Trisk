/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.controlplane.dispatcher.runner;

import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.streaming.controlplane.dispatcher.*;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Collection;

/**
 * Factory for the {@link DefaultStreamManagerDispatcherGatewayService}.
 */
class DefaultStreamManagerDispatcherGatewayServiceFactory implements AbstractStreamManagerDispatcherLeaderProcess.StreamManagerDispatcherGatewayServiceFactory {

	private final StreamManagerDispatcherFactory smDispatcherFactory;

	private final RpcService rpcService;

	private final PartialStreamManagerDispatcherServices partialSmDispatcherServices;

	DefaultStreamManagerDispatcherGatewayServiceFactory(
		StreamManagerDispatcherFactory smDispatcherFactory,
		RpcService rpcService,
		PartialStreamManagerDispatcherServices partialSmDispatcherServices) {
		this.smDispatcherFactory = smDispatcherFactory;
		this.rpcService = rpcService;
		this.partialSmDispatcherServices = partialSmDispatcherServices;
	}

	@Override
	public AbstractStreamManagerDispatcherLeaderProcess.StreamManagerDispatcherGatewayService create(
			StreamManagerDispatcherId fencingToken,
			Collection<JobGraph> recoveredJobs,
			JobGraphWriter jobGraphWriter) {
		final StreamManagerDispatcher smDispatcher;
		try {
			smDispatcher = smDispatcherFactory.createStreamManagerDispatcher(
				rpcService,
				fencingToken,
				recoveredJobs,
				PartialStreamManagerDispatcherServicesWithJobGraphStore.from(partialSmDispatcherServices, jobGraphWriter)
			);
		} catch (Exception e) {
			throw new FlinkRuntimeException("Could not create the Dispatcher rpc endpoint.", e);
		}

		smDispatcher.start();

		return DefaultStreamManagerDispatcherGatewayService.from(smDispatcher);
	}
}
