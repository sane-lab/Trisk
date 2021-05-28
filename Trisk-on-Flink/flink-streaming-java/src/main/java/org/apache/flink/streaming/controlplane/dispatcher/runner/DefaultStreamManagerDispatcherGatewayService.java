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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.streaming.controlplane.dispatcher.StreamManagerDispatcher;
import org.apache.flink.streaming.controlplane.dispatcher.StreamManagerDispatcherGateway;

import java.util.concurrent.CompletableFuture;

class DefaultStreamManagerDispatcherGatewayService implements AbstractStreamManagerDispatcherLeaderProcess.StreamManagerDispatcherGatewayService {

	private final StreamManagerDispatcher smDispatcher;
	private final StreamManagerDispatcherGateway dispatcherGateway;

	private DefaultStreamManagerDispatcherGatewayService(StreamManagerDispatcher smDispatcher) {
		this.smDispatcher = smDispatcher;
		this.dispatcherGateway = smDispatcher.getSelfGateway(StreamManagerDispatcherGateway.class);
	}

	@Override
	public StreamManagerDispatcherGateway getGateway() {
		return dispatcherGateway;
	}

	@Override
	public CompletableFuture<Void> onRemovedJobGraph(JobID jobId) {
		return smDispatcher.onRemovedJobGraph(jobId);
	}

	@Override
	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return smDispatcher.getShutDownFuture();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return smDispatcher.closeAsync();
	}

	public static DefaultStreamManagerDispatcherGatewayService from(StreamManagerDispatcher dispatcher) {
		return new DefaultStreamManagerDispatcherGatewayService(dispatcher);
	}
}
