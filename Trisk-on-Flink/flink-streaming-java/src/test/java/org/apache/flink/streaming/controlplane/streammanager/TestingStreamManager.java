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

package org.apache.flink.streaming.controlplane.streammanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

import java.util.concurrent.CompletableFuture;

public class TestingStreamManager extends StreamManager {

	private final CompletableFuture<Acknowledge> isRegisterJob;

	public TestingStreamManager(RpcService rpcService,
								StreamManagerConfiguration streamManagerConfiguration,
								ResourceID resourceId,
								JobGraph jobGraph,
								ClassLoader userLoader,
								HighAvailabilityServices highAvailabilityService,
								JobLeaderIdService jobLeaderIdService,
								LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
								FatalErrorHandler fatalErrorHandler,
								CompletableFuture<Acknowledge> isRegisterJob) throws Exception {
		super(rpcService,
			streamManagerConfiguration,
			resourceId,
			jobGraph,
			userLoader,
			highAvailabilityService,
			jobLeaderIdService,
			dispatcherGatewayRetriever,
			null,
			null,
			fatalErrorHandler);
		this.isRegisterJob = isRegisterJob;
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerJobManager(JobMasterId jobMasterId, ResourceID jobManagerResourceId, String jobManagerAddress, JobID jobId ,Time timeout) {
		isRegisterJob.complete(Acknowledge.get());
		return super.registerJobManager(jobMasterId, jobManagerResourceId, jobManagerAddress, jobId, timeout);
	}
}
