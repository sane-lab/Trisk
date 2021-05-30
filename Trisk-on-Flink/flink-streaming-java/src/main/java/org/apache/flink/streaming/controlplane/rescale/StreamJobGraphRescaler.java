/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.controlplane.rescale;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.*;
import org.apache.flink.runtime.rescale.JobGraphRescaler;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.runtime.partitioner.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamJobGraphRescaler extends JobGraphRescaler {

	static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.streaming.controlplane.rescale.StreamJobGraphRescaler.class);

	public StreamJobGraphRescaler(JobGraph jobGraph, ClassLoader userCodeLoader) {
		super(jobGraph, userCodeLoader);
	}

	@Override
	public void rescale(JobVertexID id, int newParallelism, Map<Integer, List<Integer>> partitionAssignment, List<JobVertexID> involvedUpstream, List<JobVertexID> involvedDownstream) {
		JobVertex vertex = jobGraph.findVertexByID(id);
		vertex.setParallelism(newParallelism);

		repartition(id, partitionAssignment, involvedUpstream, involvedDownstream);
	}

	@Override
	public void repartition(JobVertexID id, Map<Integer, List<Integer>> partitionAssignment, List<JobVertexID> involvedUpstream, List<JobVertexID> involvedDownstream) {
		JobVertex vertex = jobGraph.findVertexByID(id);

		StreamConfig config = new StreamConfig(vertex.getConfiguration());

		// update upstream vertices and edges
		List<StreamEdge> targetInEdges = config.getInPhysicalEdges(userCodeLoader);

		for (JobEdge jobEdge : vertex.getInputs()) {
			JobVertex upstream = jobEdge.getSource().getProducer();
			involvedUpstream.add(upstream.getID());

			StreamConfig upstreamConfig = new StreamConfig(upstream.getConfiguration());

			List<StreamEdge> upstreamOutEdges = upstreamConfig.getOutEdgesInOrder(userCodeLoader);
			Map<String, StreamEdge> updatedEdges = updateEdgePartition(
				jobEdge, partitionAssignment, upstreamOutEdges, targetInEdges);

			upstreamConfig.setOutEdgesInOrder(upstreamOutEdges);
			updateAllOperatorsConfig(upstreamConfig, updatedEdges);
		}
		config.setInPhysicalEdges(targetInEdges);

		// update downstream vertices and edges
		List<StreamEdge> targetOutEdges = config.getOutEdgesInOrder(userCodeLoader);

		for (IntermediateDataSet dataset : vertex.getProducedDataSets()) {
			for (JobEdge jobEdge : dataset.getConsumers()) {
				JobVertex downstream = jobEdge.getTarget();
				involvedDownstream.add(downstream.getID());

				StreamConfig downstreamConfig = new StreamConfig(downstream.getConfiguration());

				List<StreamEdge> downstreamInEdges = downstreamConfig.getInPhysicalEdges(userCodeLoader);
				// TODO: targetVertex is not supposed to be modified
				Map<String, StreamEdge> updatedEdges = updateEdgePartition(
					jobEdge, null, targetOutEdges, downstreamInEdges);

				downstreamConfig.setInPhysicalEdges(downstreamInEdges);
				updateAllOperatorsConfig(downstreamConfig, updatedEdges);
			}
		}
		config.setOutEdgesInOrder(targetOutEdges);
	}

	@Override
	public String print(Configuration config) {
		StreamConfig streamConfig = new StreamConfig(config);

		return streamConfig.getOutEdgesInOrder(userCodeLoader).get(0).toString();
	}

	private Map<String, StreamEdge> updateEdgePartition(
			JobEdge jobEdge,
			Map<Integer, List<Integer>> partitionAssignment,
			List<StreamEdge> upstreamOutEdges,
			List<StreamEdge> downstreamInEdges) {

		Map<String, StreamEdge> updatedEdges = new HashMap<>();

		for (StreamEdge outEdge : upstreamOutEdges) {
			for (StreamEdge inEdge : downstreamInEdges) {
				if (outEdge.equals(inEdge)) {
				}
			}
		}
		return updatedEdges;
	}

	private void updateAllOperatorsConfig(StreamConfig chainEntryPointConfig, Map<String, StreamEdge> updatedEdges) {

	}

	private void updateOperatorConfig(StreamConfig operatorConfig, Map<String, StreamEdge> updatedEdges) {
	}
}
