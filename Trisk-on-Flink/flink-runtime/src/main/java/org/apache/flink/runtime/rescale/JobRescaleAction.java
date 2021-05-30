package org.apache.flink.runtime.rescale;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;

public interface JobRescaleAction {

	JobGraph getJobGraph();

	void repartition(JobVertexID vertexID, JobRescalePartitionAssignment jobRescalePartitionAssignment);

	void scaleOut(JobVertexID vertexID, int newParallelism, JobRescalePartitionAssignment jobRescalePartitionAssignment);

	void scaleIn(JobVertexID vertexID, int newParallelism, JobRescalePartitionAssignment jobRescalePartitionAssignment);

	enum ActionType {
		REPARTITION,
		SCALE_OUT,
		SCALE_IN
	}

	default void parseParams(RescaleParamsWrapper wrapper) {
		switch (wrapper.type) {
			case REPARTITION:
				repartition(wrapper.vertexID, wrapper.jobRescalePartitionAssignment);
				break;
			case SCALE_OUT:
				scaleOut(wrapper.vertexID, wrapper.newParallelism, wrapper.jobRescalePartitionAssignment);
				break;
			case SCALE_IN:
				scaleIn(wrapper.vertexID, wrapper.newParallelism, wrapper.jobRescalePartitionAssignment);
				break;
		}
	}

	class RescaleParamsWrapper {

		final ActionType type;
		final JobVertexID vertexID;
		final int newParallelism;
		final JobRescalePartitionAssignment jobRescalePartitionAssignment;

		public RescaleParamsWrapper(
				ActionType type,
				JobVertexID vertexID,
				int newParallelism,
				JobRescalePartitionAssignment jobRescalePartitionAssignment) {
			this.type = type;
			this.vertexID = vertexID;
			this.newParallelism = newParallelism;
			this.jobRescalePartitionAssignment = jobRescalePartitionAssignment;
		}
	}
}
