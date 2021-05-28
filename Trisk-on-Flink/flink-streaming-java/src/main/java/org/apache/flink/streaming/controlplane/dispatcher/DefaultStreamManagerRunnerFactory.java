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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerConfiguration;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerRunner;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerRunnerImpl;
import org.apache.flink.streaming.controlplane.streammanager.factories.DefaultStreamManagerServiceFactory;
import org.apache.flink.streaming.controlplane.streammanager.factories.StreamManagerServiceFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author trx
 * Singleton default factory for {@link StreamManagerRunner}
 */
public enum DefaultStreamManagerRunnerFactory implements StreamManagerRunnerFactory {
    INSTANCE;

    @Override
    public StreamManagerRunnerImpl createStreamManagerRunner(
		JobGraph jobGraph,
		Configuration configuration,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
		LibraryCacheManager libraryCacheManager,
		FatalErrorHandler fatalErrorHandler) throws Exception {

        final StreamManagerConfiguration streamManagerConfiguration = StreamManagerConfiguration.fromConfiguration(configuration);

        final StreamManagerServiceFactory streamManagerServiceFactory = new DefaultStreamManagerServiceFactory(
                streamManagerConfiguration,
                rpcService,
                highAvailabilityServices,
				libraryCacheManager,
				dispatcherGatewayRetriever,
                fatalErrorHandler
        );

        // TODO: move in StreamManagerSharedServices
        final ScheduledExecutorService futureExecutor = Executors.newScheduledThreadPool(
                Hardware.getNumberCPUCores(),
                new ExecutorThreadFactory("streammanager-future"));

        return new StreamManagerRunnerImpl(
                jobGraph,
                streamManagerServiceFactory,
                highAvailabilityServices,
				libraryCacheManager,
                futureExecutor,
                fatalErrorHandler);
    }
}
