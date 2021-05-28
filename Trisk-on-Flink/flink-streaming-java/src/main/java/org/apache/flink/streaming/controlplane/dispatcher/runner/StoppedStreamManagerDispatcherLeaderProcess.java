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

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.streaming.controlplane.dispatcher.StreamManagerDispatcherGateway;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunner;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * {@link StreamManagerDispatcherLeaderProcess} implementation which is stopped. This class
 * is useful as the initial state of the {@link DefaultDispatcherRunner}.
 */
public enum StoppedStreamManagerDispatcherLeaderProcess implements StreamManagerDispatcherLeaderProcess {
	INSTANCE;

	private static final CompletableFuture<Void> TERMINATION_FUTURE = CompletableFuture.completedFuture(null);

	@Override
	public void start() {
		throw new UnsupportedOperationException("This method should never be called.");
	}

	@Override
	public UUID getLeaderSessionId() {
		throw new UnsupportedOperationException("This method should never be called.");
	}

	@Override
	public CompletableFuture<StreamManagerDispatcherGateway> getStreamManagerDispatcherGateway() {
		throw new UnsupportedOperationException("This method should never be called.");
	}

	@Override
	public CompletableFuture<String> getLeaderAddressFuture() {
		throw new UnsupportedOperationException("This method should never be called.");
	}

	@Override
	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		throw new UnsupportedOperationException("This method should never be called.");
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return TERMINATION_FUTURE;
	}
}
