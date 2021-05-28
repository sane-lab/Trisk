package org.apache.flink.runtime.rescale;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.List;

public interface JobRescaleAction {

	JobGraph getJobGraph();

	void repartition(JobVertexID vertexID,
					 JobRescalePartitionAssignment jobRescalePartitionAssignment,
					 JobGraph jobGraph,
					 List<JobVertexID> involvedUpStream,
					 List<JobVertexID> involvedDownStream);

	void scaleOut(JobVertexID vertexID,
				  int newParallelism,
				  JobRescalePartitionAssignment jobRescalePartitionAssignment,
				  JobGraph jobGraph,
				  List<JobVertexID> involvedUpStream,
				  List<JobVertexID> involvedDownStream);

	void scaleIn(JobVertexID vertexID,
				 int newParallelism,
				 JobRescalePartitionAssignment jobRescalePartitionAssignment,
				 JobGraph jobGraph,
				 List<JobVertexID> involvedUpStream,
				 List<JobVertexID> involvedDownStream);

	enum ActionType {
		REPARTITION,
		SCALE_OUT,
		SCALE_IN
	}

//	@Deprecated
//	default void parseParams(RescaleParamsWrapper wrapper) {
//		switch (wrapper.type) {
//			case REPARTITION:
//				repartition(wrapper.vertexID, wrapper.jobRescalePartitionAssignment);
//				break;
//			case SCALE_OUT:
//				scaleOut(wrapper.vertexID, wrapper.newParallelism, wrapper.jobRescalePartitionAssignment);
//				break;
//			case SCALE_IN:
//				scaleIn(wrapper.vertexID, wrapper.newParallelism, wrapper.jobRescalePartitionAssignment);
//				break;
//		}
//	}

	class RescaleParamsWrapper {

		public final ActionType type;
		public final JobVertexID vertexID;
		public final int newParallelism;
		public final JobRescalePartitionAssignment jobRescalePartitionAssignment;

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
