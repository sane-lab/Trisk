package org.apache.flink.runtime.rescale.streamswitch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rescale.controller.OperatorController;

public interface FlinkOperatorController extends OperatorController {

	void initMetrics(JobGraph jobGraph, JobVertexID vertexID, Configuration jobConfiguration, int parallelism);

	void onForceRetrieveMetrics();
}
