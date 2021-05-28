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
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.streaming.controlplane.dispatcher.PartialStreamManagerDispatcherServices;
import org.apache.flink.streaming.controlplane.dispatcher.StreamManagerDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.*;
import org.apache.flink.runtime.entrypoint.component.JobGraphRetriever;
import org.apache.flink.runtime.jobmanager.JobGraphStoreFactory;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.Executor;

/**
 * {@link DispatcherRunnerFactory} implementation which creates {@link DefaultDispatcherRunner}
 * instances.
 */
public class DefaultStreamManagerDispatcherRunnerFactory implements StreamManagerDispatcherRunnerFactory {
	private final StreamManagerDispatcherLeaderProcessFactoryFactory smDispatcherLeaderProcessFactoryFactory;

	private DefaultStreamManagerDispatcherRunnerFactory(StreamManagerDispatcherLeaderProcessFactoryFactory smDispatcherLeaderProcessFactoryFactory) {
		this.smDispatcherLeaderProcessFactoryFactory = smDispatcherLeaderProcessFactoryFactory;
	}

	@Override
	public StreamManagerDispatcherRunner createStreamManagerDispatcherRunner(
		LeaderElectionService leaderElectionService,
		FatalErrorHandler fatalErrorHandler,
		JobGraphStoreFactory jobGraphStoreFactory,
		Executor ioExecutor,
		RpcService rpcService,
		PartialStreamManagerDispatcherServices partialDispatcherServices) throws Exception {

		final StreamManagerDispatcherLeaderProcessFactory smDispatcherLeaderProcessFactory = smDispatcherLeaderProcessFactoryFactory.createFactory(
			jobGraphStoreFactory,
			ioExecutor,
			rpcService,
			partialDispatcherServices,
			fatalErrorHandler
		);

		return DefaultStreamManagerDispatcherRunner.create(
			leaderElectionService,
			fatalErrorHandler,
			smDispatcherLeaderProcessFactory);
	}

	public static DefaultStreamManagerDispatcherRunnerFactory createSessionRunner(StreamManagerDispatcherFactory dispatcherFactory) {
		return new DefaultStreamManagerDispatcherRunnerFactory(
			SessionStreamManagerDispatcherLeaderProcessFactoryFactory.create(dispatcherFactory));
	}

	public static DefaultStreamManagerDispatcherRunnerFactory createJobRunner(JobGraphRetriever jobGraphRetriever) {
		return new DefaultStreamManagerDispatcherRunnerFactory(
			JobStreamManagerDispatcherLeaderProcessFactoryFactory.create(jobGraphRetriever));
	}
}
