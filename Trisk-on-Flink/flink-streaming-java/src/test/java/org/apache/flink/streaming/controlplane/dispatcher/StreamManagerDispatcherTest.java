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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.dispatcher.*;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmaster.*;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.streaming.controlplane.streammanager.*;
import org.apache.flink.streaming.controlplane.streammanager.factories.StreamManagerServiceFactory;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.ThrowingRunnable;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.*;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Test for the {@link Dispatcher} component.
 */
public class StreamManagerDispatcherTest extends TestLogger {

	private static RpcService rpcService;

	private static final Time TIMEOUT = Time.seconds(10L);

	private static final JobID TEST_JOB_ID = new JobID();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public TestName name = new TestName();

	private JobGraph jobGraph;

	private TestingFatalErrorHandler fatalErrorHandler;

	private LeaderElectionService jobMasterLeaderElectionService;

	private CountDownLatch createdJobManagerRunnerLatch;

	private Configuration configuration;

	private BlobServer blobServer;

	/**
	 * Instance under test.
	 */
	private TestingStreamManagerDispatcher dispatcher;

	private TestingDispatcher runtimeDispatcher;

	private TestingHighAvailabilityServices haServices;

	private HeartbeatServices heartbeatServices;

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();
	}

	@AfterClass
	public static void teardownClass() throws Exception {
		if (rpcService != null) {
			RpcUtils.terminateRpcService(rpcService, TIMEOUT);

			rpcService = null;
		}
	}

	@Before
	public void setUp() throws Exception {
		final JobVertex testVertex = new JobVertex("testVertex");
		testVertex.setInvokableClass(NoOpInvokable.class);
		jobGraph = new JobGraph(TEST_JOB_ID, "testJob", testVertex);

		fatalErrorHandler = new TestingFatalErrorHandler();
		heartbeatServices = new HeartbeatServices(1000L, 10000L);

		jobMasterLeaderElectionService = new StandaloneLeaderElectionService();

		haServices = new TestingHighAvailabilityServicesBuilder().build();
		haServices.setJobMasterLeaderElectionService(TEST_JOB_ID, jobMasterLeaderElectionService);
		haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());
		haServices.setResourceManagerLeaderRetriever(new SettableLeaderRetrievalService());

		configuration = new Configuration();

		configuration.setString(
			BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());

		createdJobManagerRunnerLatch = new CountDownLatch(2);
		blobServer = new BlobServer(configuration, new VoidBlobStore());
	}

	@Nonnull
	private TestingStreamManagerDispatcher createAndStartDispatcher(
		HeartbeatServices heartbeatServices,
		TestingHighAvailabilityServices haServices,
		StreamManagerRunnerFactory streamManagerRunnerFactory) throws Exception {
		TestingDispatcherBuilder dispatcherBuilder = new TestingDispatcherBuilder()
			.setHaServices(haServices)
			.setHeartbeatServices(heartbeatServices)
			.setStreamManagerRunnerFactory(streamManagerRunnerFactory);

		runtimeDispatcher = dispatcherBuilder.buildRuntimeDispatcher();

		runtimeDispatcher.start();
		haServices.setDispatcherLeaderRetriever(
			new StandaloneLeaderRetrievalService(runtimeDispatcher.getAddress(), runtimeDispatcher.getFencingToken().toUUID()));

		final TestingStreamManagerDispatcher streamManagerDispatcher = dispatcherBuilder.buildStreamManagerDispatcher();

		streamManagerDispatcher.start();

		return streamManagerDispatcher;
	}

	private class TestingDispatcherBuilder {

		private Collection<JobGraph> initialJobGraphs = Collections.emptyList();

		private HeartbeatServices heartbeatServices = StreamManagerDispatcherTest.this.heartbeatServices;

		private HighAvailabilityServices haServices = StreamManagerDispatcherTest.this.haServices;

		private StreamManagerRunnerFactory streamManagerRunnerFactory = DefaultStreamManagerRunnerFactory.INSTANCE;

		private JobManagerRunnerFactory jobManagerRunnerFactory = DefaultJobManagerRunnerFactory.INSTANCE;

		private JobGraphWriter jobGraphWriter = NoOpJobGraphWriter.INSTANCE;

		TestingDispatcherBuilder setHeartbeatServices(HeartbeatServices heartbeatServices) {
			this.heartbeatServices = heartbeatServices;
			return this;
		}

		TestingDispatcherBuilder setHaServices(HighAvailabilityServices haServices) {
			this.haServices = haServices;
			return this;
		}

		TestingDispatcherBuilder setInitialJobGraphs(Collection<JobGraph> initialJobGraphs) {
			this.initialJobGraphs = initialJobGraphs;
			return this;
		}

		TestingDispatcherBuilder setStreamManagerRunnerFactory(StreamManagerRunnerFactory streamManagerRunnerFactory) {
			this.streamManagerRunnerFactory = streamManagerRunnerFactory;
			return this;
		}

		TestingDispatcherBuilder setJobGraphWriter(JobGraphWriter jobGraphWriter) {
			this.jobGraphWriter = jobGraphWriter;
			return this;
		}

		TestingStreamManagerDispatcher buildStreamManagerDispatcher() throws Exception {
			TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();

			final MemoryArchivedExecutionGraphStore archivedExecutionGraphStore = new MemoryArchivedExecutionGraphStore();

			return new TestingStreamManagerDispatcher(
				rpcService,
				StreamManagerDispatcher.DISPATCHER_NAME + '_' + name.getMethodName(),
				StreamManagerDispatcherId.generate(),
				initialJobGraphs,
				new StreamManagerDispatcherServices(
					configuration,
					haServices,
					blobServer,
					heartbeatServices,
					fatalErrorHandler,
					jobGraphWriter,
					streamManagerRunnerFactory)
			);
		}

		TestingDispatcher buildRuntimeDispatcher() throws Exception {
			TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();

			final MemoryArchivedExecutionGraphStore archivedExecutionGraphStore = new MemoryArchivedExecutionGraphStore();

			return new TestingDispatcher(
				rpcService,
				Dispatcher.DISPATCHER_NAME + '_' + name.getMethodName(),
				DispatcherId.generate(),
				initialJobGraphs,
				new DispatcherServices(
					configuration,
					haServices,
					() -> CompletableFuture.completedFuture(resourceManagerGateway),
					blobServer,
					heartbeatServices,
					archivedExecutionGraphStore,
					fatalErrorHandler,
					VoidHistoryServerArchivist.INSTANCE,
					null,
					UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
					jobGraphWriter,
					jobManagerRunnerFactory));
		}
	}

	@After
	public void tearDown() throws Exception {
		try {
			fatalErrorHandler.rethrowError();
		} finally {
			if (dispatcher != null) {
				RpcUtils.terminateRpcEndpoint(dispatcher, TIMEOUT);
			}
		}

		if (haServices != null) {
			haServices.closeAndCleanupAllData();
		}

		if (blobServer != null) {
			blobServer.close();
		}
	}

	/**
	 * Tests that stream manager could get runtime dispatcher's gateway
	 */
	@Test
	public void testGetRuntimeDispatcherGateway() throws Exception {
		dispatcher = createAndStartDispatcher(
			heartbeatServices,
			haServices,
			new ExpectedJobIdStreamManagerRunnerFactory(TEST_JOB_ID, createdJobManagerRunnerLatch));

		try {
			dispatcher.getStartedDispatcherRetriever().getFuture().get();
		} catch (Exception e) {
			e.printStackTrace();
			fail("can not get runtime dispatcher gateway: " + e.getMessage());
		}
	}


	/**
	 * 1. Tests that we can submit a job to the StreamManagerDispatcher which then spawns a
	 * new StreamManagerRunner.
	 * 2. Test that job master could connect to stream manager
	 */
	@Test
	public void testJobSubmission() throws Exception {
		CompletableFuture<Acknowledge> isRegisterJobManagerFuture = new CompletableFuture<>();

		dispatcher = createAndStartDispatcher(heartbeatServices, haServices,
			new ExpectedCompletableFutureStreamManagerRunnerFactory(isRegisterJobManagerFuture));

		StreamManagerDispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(StreamManagerDispatcherGateway.class);

		CompletableFuture<Acknowledge> acknowledgeFuture = dispatcherGateway.submitJob(jobGraph, RpcUtils.INF_TIMEOUT);

		assertEquals("stream manager runner do not start", Acknowledge.get(), acknowledgeFuture.get(3, TimeUnit.SECONDS));
		assertEquals("job master can not connect to stream manager", Acknowledge.get(), isRegisterJobManagerFuture.get(3, TimeUnit.HOURS));
	}

	@Test
	public void testOnRemovedJobGraphDoesNotCleanUpHAFiles() throws Exception {
		final CompletableFuture<JobID> removeJobGraphFuture = new CompletableFuture<>();
		final TestingJobGraphStore testingJobGraphStore = TestingJobGraphStore.newBuilder()
			.setRemoveJobGraphConsumer(removeJobGraphFuture::complete)
			.build();

		dispatcher = new TestingDispatcherBuilder()
			.setInitialJobGraphs(Collections.singleton(jobGraph))
			.setJobGraphWriter(testingJobGraphStore)
			.buildStreamManagerDispatcher();
		dispatcher.start();

		final CompletableFuture<Void> processFuture = dispatcher.onRemovedJobGraph(jobGraph.getJobID());

		processFuture.join();

		try {
			removeJobGraphFuture.get(10L, TimeUnit.MILLISECONDS);
			fail("onRemovedJobGraph should not remove the job from the JobGraphStore.");
		} catch (TimeoutException expected) {
		}
	}

	private static final class BlockingJobManagerRunnerFactory extends TestingJobManagerRunnerFactory {

		@Nonnull
		private final ThrowingRunnable<Exception> jobManagerRunnerCreationLatch;

		BlockingJobManagerRunnerFactory(@Nonnull ThrowingRunnable<Exception> jobManagerRunnerCreationLatch) {
			this.jobManagerRunnerCreationLatch = jobManagerRunnerCreationLatch;
		}

		@Override
		public TestingJobManagerRunner createJobManagerRunner(JobGraph jobGraph, Configuration configuration, RpcService rpcService, HighAvailabilityServices highAvailabilityServices, HeartbeatServices heartbeatServices, JobManagerSharedServices jobManagerSharedServices, JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory, FatalErrorHandler fatalErrorHandler) throws Exception {
			jobManagerRunnerCreationLatch.run();

			return super.createJobManagerRunner(jobGraph, configuration, rpcService, highAvailabilityServices, heartbeatServices, jobManagerSharedServices, jobManagerJobMetricGroupFactory, fatalErrorHandler);
		}
	}

	private JobGraph createFailingJobGraph(Exception failureCause) {
		final FailingJobVertex jobVertex = new FailingJobVertex("Failing JobVertex", failureCause);
		jobVertex.setInvokableClass(NoOpInvokable.class);
		return new JobGraph(jobGraph.getJobID(), "Failing JobGraph", jobVertex);
	}

	private static class FailingJobVertex extends JobVertex {

		private static final long serialVersionUID = 3218428829168840760L;

		private final Exception failure;

		private FailingJobVertex(String name, Exception failure) {
			super(name);
			this.failure = failure;
		}

		@Override
		public void initializeOnMaster(ClassLoader loader) throws Exception {
			throw failure;
		}
	}

	private static final class ExpectedCompletableFutureStreamManagerRunnerFactory implements StreamManagerRunnerFactory {

		private final CompletableFuture<Acknowledge> acknowledgeCompletableFuture;

		private ExpectedCompletableFutureStreamManagerRunnerFactory(CompletableFuture<Acknowledge> acknowledgeCompletableFuture) {
			this.acknowledgeCompletableFuture = acknowledgeCompletableFuture;
		}

		@Override
		public StreamManagerRunnerImpl createStreamManagerRunner(
			JobGraph jobGraph,
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
			LibraryCacheManager libraryCacheManager,
			BlobWriter blobWriter,
			FatalErrorHandler fatalErrorHandler) throws Exception {

			final StreamManagerConfiguration streamManagerConfiguration = StreamManagerConfiguration.fromConfiguration(configuration);

			final StreamManagerServiceFactory streamManagerServiceFactory = (jobGraph1, userCodeLoader) -> {
				final StreamManagerRuntimeServices streamManagerRuntimeServices = StreamManagerRuntimeServices.fromConfiguration(
					streamManagerConfiguration,
					highAvailabilityServices,
					rpcService.getScheduledExecutor());

				return new TestingStreamManager(
					rpcService,
					streamManagerConfiguration,
					ResourceID.generate(),
					jobGraph1,
					libraryCacheManager.getClassLoader(jobGraph.getJobID()),
					highAvailabilityServices,
					streamManagerRuntimeServices.getJobLeaderIdService(),
					dispatcherGatewayRetriever,
					fatalErrorHandler,
					acknowledgeCompletableFuture);
			};

			final ScheduledExecutorService futureExecutor = Executors.newScheduledThreadPool(
				Hardware.getNumberCPUCores(),
				new ExecutorThreadFactory("streammanager-future"));

			return new StreamManagerRunnerImpl(
				jobGraph,
				streamManagerServiceFactory,
				highAvailabilityServices,
				libraryCacheManager, futureExecutor,
				fatalErrorHandler);
		}
	}


	private static final class ExpectedJobIdStreamManagerRunnerFactory implements StreamManagerRunnerFactory {

		private final JobID expectedJobId;

		private final CountDownLatch createdJobManagerRunnerLatch;

		private ExpectedJobIdStreamManagerRunnerFactory(JobID expectedJobId, CountDownLatch createdJobManagerRunnerLatch) {
			this.expectedJobId = expectedJobId;
			this.createdJobManagerRunnerLatch = createdJobManagerRunnerLatch;
		}

		@Override
		public StreamManagerRunnerImpl createStreamManagerRunner(
			JobGraph jobGraph,
			Configuration configuration,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
			LibraryCacheManager libraryCacheManager,
			BlobWriter blobWriter,
			FatalErrorHandler fatalErrorHandler) {

			createdJobManagerRunnerLatch.countDown();

			try {
				return DefaultStreamManagerRunnerFactory.INSTANCE.createStreamManagerRunner(
					jobGraph,
					configuration,
					rpcService,
					highAvailabilityServices,
					dispatcherGatewayRetriever,
					null,
					null,
					fatalErrorHandler);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}
	}

}
