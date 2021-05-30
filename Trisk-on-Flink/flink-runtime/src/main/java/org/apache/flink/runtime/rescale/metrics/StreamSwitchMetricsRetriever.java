package org.apache.flink.runtime.rescale.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Map;

public interface StreamSwitchMetricsRetriever {

	void init(JobGraph jobGraph, JobVertexID vertexID, Configuration jobConfiguration, int partitions, int numPartitions);

	//Metrics should be in Map <Type of metrics, Map <ContainerId, Data> > format
	Map<String, Object> retrieveMetrics();

	JobVertexID getVertexId();
}
