package org.apache.flink.runtime.rescale.streamswitch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rescale.JobRescaleAction;
import org.apache.flink.runtime.rescale.JobRescalePartitionAssignment;
import org.apache.flink.runtime.rescale.RescaleActionQueue;
import org.apache.flink.runtime.rescale.controller.OperatorControllerListener;
import org.apache.flink.runtime.rescale.controller.OperatorController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.rescale.JobRescaleAction.ActionType.*;

public class FlinkStreamSwitchAdaptor {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkStreamSwitchAdaptor.class);

	private final RescaleActionQueue actionQueue;

	private final Map<JobVertexID, FlinkOperatorController> controllers;

	private final Configuration config;

//	private final long migrationInterval;

	public FlinkStreamSwitchAdaptor(
		JobRescaleAction rescaleAction,
		ExecutionGraph executionGraph) {

		this.actionQueue = new RescaleActionQueue(rescaleAction);

		this.controllers = new HashMap<>(executionGraph.getAllVertices().size());

		this.config = executionGraph.getJobConfiguration();

//		this.migrationInterval = config.getLong("streamswitch.system.migration_interval", 5000);

		for (Map.Entry<JobVertexID, ExecutionJobVertex> entry : executionGraph.getAllVertices().entrySet()) {
			JobVertexID vertexID = entry.getKey();
			int parallelism = entry.getValue().getParallelism();
			int maxParallelism = entry.getValue().getMaxParallelism();

			// TODO scaling: using DummyStreamSwitch for test purpose
//			if (!entry.getValue().getName().toLowerCase().contains("join") && !entry.getValue().getName().toLowerCase().contains("window")) {
//				continue;
//			}
			FlinkOperatorController controller;
//
			if (entry.getValue().getName().toLowerCase().contains("map")) {
				controller = new DummyStreamSwitch("map");
			} else if (entry.getValue().getName().toLowerCase().contains("filter")) {
				controller = new DummyStreamSwitch("filter");
			} else {
				continue;
			}
			config.setString("vertex_id", vertexID.toString());
//			FlinkOperatorController controller = new LatencyGuarantor(config);
			OperatorControllerListener listener = new OperatorControllerListenerImpl(vertexID, parallelism, maxParallelism);

			controller.init(listener, generateExecutorDelegates(parallelism), generateFinestPartitionDelegates(maxParallelism));
			controller.initMetrics(rescaleAction.getJobGraph(), vertexID, config, parallelism);

			this.controllers.put(vertexID, controller);
		}
	}

	public void startControllers() {
		actionQueue.start();

		for (OperatorController controller : controllers.values()) {
			controller.start();
		}
	}

	public void stopControllers() {
		actionQueue.stopGracefully();

		for (OperatorController controller : controllers.values()) {
			controller.stopGracefully();
		}
	}

	public void onChangeImplemented(JobVertexID jobVertexID) {
		// sleep period of time
//		try {
//			Thread.sleep(migrationInterval);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		LOG.info("++++++ onChangeImplemented triggered for jobVertex " + jobVertexID);
		this.controllers.get(jobVertexID).onMigrationCompleted();

		actionQueue.notifyFinished();
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

			if (numOpenedSubtask >= newParallelism) {
				// repartition
				jobRescalePartitionAssignment = new JobRescalePartitionAssignment(
					executorMapping, oldExecutorMapping, oldRescalePA, numOpenedSubtask);

//				rescaleAction.repartition(jobVertexID, jobRescalePartitionAssignment);
				actionQueue.put(REPARTITION, jobVertexID, -1, jobRescalePartitionAssignment);
			} else {
				// scale out
				jobRescalePartitionAssignment = new JobRescalePartitionAssignment(
					executorMapping, oldExecutorMapping, oldRescalePA, newParallelism);

//				rescaleAction.scaleOut(jobVertexID, newParallelism, jobRescalePartitionAssignment);
				actionQueue.put(SCALE_OUT, jobVertexID, newParallelism, jobRescalePartitionAssignment);
				numOpenedSubtask = newParallelism;
			}

			this.oldRescalePA = jobRescalePartitionAssignment;
			this.oldExecutorMapping = new HashMap<>(executorMapping);
		}
	}
}
