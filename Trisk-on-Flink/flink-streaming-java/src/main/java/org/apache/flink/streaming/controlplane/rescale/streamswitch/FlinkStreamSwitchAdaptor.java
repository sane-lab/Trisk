package org.apache.flink.streaming.controlplane.rescale.streamswitch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rescale.JobRescalePartitionAssignment;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.controlplane.rescale.RescaleActionConsumer;
import org.apache.flink.streaming.controlplane.rescale.controller.OperatorController;
import org.apache.flink.streaming.controlplane.rescale.controller.OperatorControllerListener;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.apache.flink.streaming.controlplane.udm.ControlPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.rescale.JobRescaleAction.ActionType.*;

public class FlinkStreamSwitchAdaptor implements ControlPolicy {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkStreamSwitchAdaptor.class);

	private final RescaleActionConsumer actionConsumer;

	private final Map<JobVertexID, FlinkOperatorController> controllers;

	private final Configuration config;

//	private final long migrationInterval;

	public FlinkStreamSwitchAdaptor(
		ReconfigurationExecutor reconfigurationExecutor,
		JobGraph jobGraph) {

		this.actionConsumer = new RescaleActionConsumer(reconfigurationExecutor, this);

		this.controllers = new HashMap<>(jobGraph.getNumberOfVertices());

		this.config = jobGraph.getJobConfiguration();

//		this.migrationInterval = config.getLong("streamswitch.system.migration_interval", 5000);

		for (JobVertex jobVertex : jobGraph.getVertices()) {
			JobVertexID vertexID = jobVertex.getID();
			int parallelism = jobVertex.getParallelism();
			int maxParallelism = getMaxParallelism(jobVertex);

			// TODO scaling: using DummyStreamSwitch for test purpose
//			if (!entry.getValue().getName().toLowerCase().contains("join") && !entry.getValue().getName().toLowerCase().contains("window")) {
//				continue;
//			}
			FlinkOperatorController controller;

			if (jobVertex.getName().toLowerCase().contains("map")) {
				controller = new DummyStreamSwitch("map");
//				continue;
			} else if (jobVertex.getName().toLowerCase().contains("filter")) {
//				controller = new DummyStreamSwitch("filter");
				continue;
			} else {
				controller = ConfigurableDummyStreamSwitch.createFromJobVertex(jobVertex.getName());
				if (controller == null) {
					continue;
				}
			}
			config.setString("vertex_id", vertexID.toString());
//			FlinkOperatorController controller = new LatencyGuarantor(config);
			OperatorControllerListener listener = new OperatorControllerListenerImpl(vertexID, parallelism, maxParallelism);

			controller.init(listener, generateExecutorDelegates(parallelism), generateFinestPartitionDelegates(maxParallelism));
			controller.initMetrics(jobGraph, vertexID, config, parallelism);

			this.controllers.put(vertexID, controller);
		}
	}

	private static int getMaxParallelism(JobVertex jobVertex) {
		final int vertexParallelism = jobVertex.getParallelism();
		final int defaultParallelism = 1;
		int numTaskVertices = vertexParallelism > 0 ? vertexParallelism : defaultParallelism;

		final int configuredMaxParallelism = jobVertex.getMaxParallelism();

		// if no max parallelism was configured by the user, we calculate and set a default
		return configuredMaxParallelism != -1 ?
			configuredMaxParallelism : KeyGroupRangeAssignment.computeDefaultMaxParallelism(numTaskVertices);
	}

	@Override
	public void startControllers() {
		new Thread(actionConsumer).start();

		for (OperatorController controller : controllers.values()) {
			controller.start();
		}
	}

	@Override
	public void stopControllers() {
		actionConsumer.stopGracefully();

		for (OperatorController controller : controllers.values()) {
			controller.stopGracefully();
		}
	}

	@Override
	public void onChangeStarted() throws InterruptedException {

	}

	@Override
	public void onChangeCompleted(Throwable throwable) {

	}

	public void onChangeCompleted(JobVertexID jobVertexID) {
		this.onMigrationExecutorsStopped(jobVertexID);

		LOG.info("++++++ onChangeImplemented triggered for jobVertex " + jobVertexID);
		this.controllers.get(jobVertexID).onMigrationCompleted();

		actionConsumer.notifyFinished();
	}

	public void onForceRetrieveMetrics(JobVertexID jobVertexID) {
		LOG.info("++++++ onForceRetrieveMetrics triggered for jobVertex " + jobVertexID);
		this.controllers.get(jobVertexID).onForceRetrieveMetrics();
	}

	public void onMigrationExecutorsStopped(JobVertexID jobVertexID) {
		LOG.info("++++++ onMigrationExecutorsStopped triggered for jobVertex " + jobVertexID);
		this.controllers.get(jobVertexID).onMigrationExecutorsStopped();
	}

	private static List<String> generateExecutorDelegates(int parallelism) {
		List<String> executors = new ArrayList<>();
		for (int i = 0; i < parallelism; i++) {
			executors.add(String.valueOf(i));
		}
		return executors;
	}

	private static List<String> generateFinestPartitionDelegates(int maxParallelism) {
		List<String> finestPartitions = new ArrayList<>();
		for (int i = 0; i < maxParallelism; i++) {
			finestPartitions.add(String.valueOf(i));
		}
		return finestPartitions;
	}

	private class OperatorControllerListenerImpl implements OperatorControllerListener {

		public final JobVertexID jobVertexID;

		private int numOpenedSubtask;

		private JobRescalePartitionAssignment oldRescalePA;

		private Map<String, List<String>> oldExecutorMapping;

		public OperatorControllerListenerImpl(JobVertexID jobVertexID, int parallelism, int maxParallelism) {
			this.jobVertexID = jobVertexID;
			this.numOpenedSubtask = parallelism;
		}

		@Override
		public void setup(Map<String, List<String>> executorMapping) {
			this.oldRescalePA = new JobRescalePartitionAssignment(executorMapping, numOpenedSubtask);
			this.oldExecutorMapping = new HashMap<>(executorMapping);
		}

		@Override
		public void remap(Map<String, List<String>> executorMapping) {
			handleTreatment(executorMapping);
		}

		@Override
		public void scale(int newParallelism, Map<String, List<String>> executorMapping) {
			handleTreatment(executorMapping);
		}

		private void handleTreatment(Map<String, List<String>> executorMapping) {
			int newParallelism = executorMapping.keySet().size();

			JobRescalePartitionAssignment jobRescalePartitionAssignment;

			if (numOpenedSubtask == newParallelism) {
				// repartition
				jobRescalePartitionAssignment = new JobRescalePartitionAssignment(
					executorMapping, oldExecutorMapping, oldRescalePA, numOpenedSubtask);

//				rescaleAction.repartition(jobVertexID, jobRescalePartitionAssignment);
				actionConsumer.put(REPARTITION, jobVertexID, -1, jobRescalePartitionAssignment);
			} else if (numOpenedSubtask > newParallelism) {
				jobRescalePartitionAssignment = new JobRescalePartitionAssignment(
					executorMapping, oldExecutorMapping, oldRescalePA, numOpenedSubtask);

//				rescaleAction.repartition(jobVertexID, jobRescalePartitionAssignment);
				actionConsumer.put(SCALE_IN, jobVertexID, newParallelism, jobRescalePartitionAssignment);
				numOpenedSubtask = newParallelism;
			}
			else {
				// scale out
				jobRescalePartitionAssignment = new JobRescalePartitionAssignment(
					executorMapping, oldExecutorMapping, oldRescalePA, newParallelism);

//				rescaleAction.scaleOut(jobVertexID, newParallelism, jobRescalePartitionAssignment);
				actionConsumer.put(SCALE_OUT, jobVertexID, newParallelism, jobRescalePartitionAssignment);
				numOpenedSubtask = newParallelism;
			}

			this.oldRescalePA = jobRescalePartitionAssignment;
			this.oldExecutorMapping = new HashMap<>(executorMapping);
		}
	}
}
