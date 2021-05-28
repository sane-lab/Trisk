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

package org.apache.flink.streaming.controlplane.webmonitor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.handler.job.JobExecutionResultHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.streaming.controlplane.rest.handler.job.RegisterStreamManagerControllerHandler;
import org.apache.flink.streaming.controlplane.rest.handler.job.StreamManagerJobExecutionResultHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Rest endpoint which serves the web frontend REST calls.
 *
 * @param <T> type of the leader gateway
 */
public class StreamManagerWebMonitorEndpoint<T extends StreamManagerRestfulGateway> extends RestServerEndpoint implements LeaderContender, JsonArchivist {

	protected final GatewayRetriever<? extends T> leaderRetriever;
	protected final Configuration clusterConfiguration;
	protected final RestHandlerConfiguration restConfiguration;
	protected final ScheduledExecutorService executor;

	private final ExecutionGraphCache executionGraphCache;

	private final LeaderElectionService leaderElectionService;

	private final FatalErrorHandler fatalErrorHandler;

	private boolean hasWebUI = false;

	private final Collection<JsonArchivist> archivingHandlers = new ArrayList<>(16);

	@Nullable
	private ScheduledFuture<?> executionGraphCleanupTask;

	public StreamManagerWebMonitorEndpoint(
		RestServerEndpointConfiguration endpointConfiguration,
		GatewayRetriever<? extends T> leaderRetriever,
		Configuration clusterConfiguration,
		RestHandlerConfiguration restConfiguration,
		ScheduledExecutorService executor,
		LeaderElectionService leaderElectionService,
		ExecutionGraphCache executionGraphCache,
		FatalErrorHandler fatalErrorHandler) throws IOException {
		super(endpointConfiguration);
		this.leaderRetriever = Preconditions.checkNotNull(leaderRetriever);
		this.clusterConfiguration = Preconditions.checkNotNull(clusterConfiguration);
		this.restConfiguration = Preconditions.checkNotNull(restConfiguration);
		this.executor = Preconditions.checkNotNull(executor);

		this.executionGraphCache = executionGraphCache;

		this.leaderElectionService = Preconditions.checkNotNull(leaderElectionService);
		this.fatalErrorHandler = Preconditions.checkNotNull(fatalErrorHandler);
	}

	@Override
	protected List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(final CompletableFuture<String> localAddressFuture) {
		ArrayList<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> handlers = new ArrayList<>(2);

		final Time timeout = restConfiguration.getTimeout();

		// TODO: check if we need any handlers here.
		final StreamManagerJobExecutionResultHandler jobExecutionResultHandler = new StreamManagerJobExecutionResultHandler(
			leaderRetriever,
			timeout,
			responseHeaders);

		final RegisterStreamManagerControllerHandler registerStreamManagerControllerHandler = new RegisterStreamManagerControllerHandler(
			leaderRetriever,
			timeout,
			responseHeaders);

		handlers.add(Tuple2.of(jobExecutionResultHandler.getMessageHeaders(), jobExecutionResultHandler));
		handlers.add(Tuple2.of(registerStreamManagerControllerHandler.getMessageHeaders(), registerStreamManagerControllerHandler));

		return handlers;
	}


	@Override
	public void startInternal() throws Exception {
		leaderElectionService.start(this);
		startExecutionGraphCacheCleanupTask();

		if (hasWebUI) {
			log.info("Web frontend listening at {}.", getRestBaseUrl());
		}
	}

	private void startExecutionGraphCacheCleanupTask() {
		final long cleanupInterval = 2 * restConfiguration.getRefreshInterval();
		executionGraphCleanupTask = executor.scheduleWithFixedDelay(
			executionGraphCache::cleanup,
			cleanupInterval,
			cleanupInterval,
			TimeUnit.MILLISECONDS);
	}

	@Override
	protected CompletableFuture<Void> shutDownInternal() {
		if (executionGraphCleanupTask != null) {
			executionGraphCleanupTask.cancel(false);
		}

		executionGraphCache.close();

		final CompletableFuture<Void> shutdownFuture = FutureUtils.runAfterwards(
			super.shutDownInternal(),
			() -> ExecutorUtils.gracefulShutdown(10, TimeUnit.SECONDS, executor));

		final File webUiDir = restConfiguration.getWebUiDir();

		return FutureUtils.runAfterwardsAsync(
			shutdownFuture,
			() -> {
				Exception exception = null;
				try {
					log.info("Removing cache directory {}", webUiDir);
					FileUtils.deleteDirectory(webUiDir);
				} catch (Exception e) {
					exception = e;
				}

				try {
					leaderElectionService.stop();
				} catch (Exception e) {
					exception = ExceptionUtils.firstOrSuppressed(e, exception);
				}

				if (exception != null) {
					throw exception;
				}
			});
	}

	//-------------------------------------------------------------------------
	// LeaderContender
	//-------------------------------------------------------------------------

	@Override
	public void grantLeadership(final UUID leaderSessionID) {
		log.info("{} was granted leadership with leaderSessionID={}", getRestBaseUrl(), leaderSessionID);
		leaderElectionService.confirmLeadership(leaderSessionID, getRestBaseUrl());
	}

	@Override
	public void revokeLeadership() {
		log.info("{} lost leadership", getRestBaseUrl());
	}

	@Override
	public String getDescription() {
		return getRestBaseUrl();
	}

	@Override
	public void handleError(final Exception exception) {
		fatalErrorHandler.onFatalError(exception);
	}

	@Override
	public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
		Collection<ArchivedJson> archivedJson = new ArrayList<>(archivingHandlers.size());
		for (JsonArchivist archivist : archivingHandlers) {
			Collection<ArchivedJson> subArchive = archivist.archiveJsonWithPath(graph);
			archivedJson.addAll(subArchive);
		}
		return archivedJson;
	}

	public static ScheduledExecutorService createExecutorService(int numThreads, int threadPriority, String componentName) {
		if (threadPriority < Thread.MIN_PRIORITY || threadPriority > Thread.MAX_PRIORITY) {
			throw new IllegalArgumentException(
				String.format(
					"The thread priority must be within (%s, %s) but it was %s.",
					Thread.MIN_PRIORITY,
					Thread.MAX_PRIORITY,
					threadPriority));
		}

		return Executors.newScheduledThreadPool(
			numThreads,
			new ExecutorThreadFactory.Builder()
				.setThreadPriority(threadPriority)
				.setPoolName("Flink-" + componentName)
				.build());
	}
}
