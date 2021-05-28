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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

final class StreamManagerDispatcherRunnerLeaderElectionLifecycleManager<T extends StreamManagerDispatcherRunner & LeaderContender> implements StreamManagerDispatcherRunner {
	private final T smDispatcherRunner;
	private final LeaderElectionService leaderElectionService;

	private StreamManagerDispatcherRunnerLeaderElectionLifecycleManager(T smDispatcherRunner, LeaderElectionService leaderElectionService) throws Exception {
		this.smDispatcherRunner = smDispatcherRunner;
		this.leaderElectionService = leaderElectionService;

		leaderElectionService.start(smDispatcherRunner);
	}

	@Override
	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return smDispatcherRunner.getShutDownFuture();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		final CompletableFuture<Void> servicesTerminationFuture = stopServices();
		final CompletableFuture<Void> dispatcherRunnerTerminationFuture = smDispatcherRunner.closeAsync();

		return FutureUtils.completeAll(Arrays.asList(servicesTerminationFuture, dispatcherRunnerTerminationFuture));
	}

	private CompletableFuture<Void> stopServices() {
		try {
			leaderElectionService.stop();
		} catch (Exception e) {
			return FutureUtils.completedExceptionally(e);
		}

		return FutureUtils.completedVoidFuture();
	}

	public static <T extends StreamManagerDispatcherRunner & LeaderContender> StreamManagerDispatcherRunner createFor(T smDispatcherRunner, LeaderElectionService leaderElectionService) throws Exception {
		return new StreamManagerDispatcherRunnerLeaderElectionLifecycleManager<>(smDispatcherRunner, leaderElectionService);
	}
}
