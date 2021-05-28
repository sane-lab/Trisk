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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rescale.reconfigure.JobGraphRescaler;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.runtime.partitioner.AssignedKeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class StreamJobGraphRescaler implements JobGraphRescaler {

	static final Logger LOG = LoggerFactory.getLogger(StreamJobGraphRescaler.class);

	protected final JobGraph jobGraph;
	protected final ClassLoader userCodeLoader;

	public StreamJobGraphRescaler(JobGraph jobGraph, ClassLoader userCodeLoader) {
		this.jobGraph = jobGraph;
		this.userCodeLoader = userCodeLoader;
	}

	@Override
	public Tuple2<List<JobVertexID>, List<JobVertexID>> rescale(
		JobVertexID id,
		int newParallelism,
		Map<Integer, List<Integer>> partitionAssignment) {
		JobVertex vertex = jobGraph.findVertexByID(id);
		vertex.setParallelism(newParallelism);

		return repartition(id, partitionAssignment);
	}

	@Override
	public Tuple2<List<JobVertexID>, List<JobVertexID>> repartition(
		JobVertexID id,
		Map<Integer, List<Integer>> partitionAssignment) {
		List<JobVertexID> involvedUpstream = new LinkedList<>();
		List<JobVertexID> involvedDownstream = new LinkedList<>();
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
		return Tuple2.of(involvedUpstream, involvedDownstream);
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

					StreamPartitioner oldPartitioner = outEdge.getPartitioner();
					StreamPartitioner newPartitioner = null;

					if (oldPartitioner instanceof ForwardPartitioner) {
						System.out.println("update vertex of ForwardPartitioner");
						newPartitioner = new RebalancePartitioner();
					} else if (oldPartitioner instanceof KeyGroupStreamPartitioner && partitionAssignment != null) {
						System.out.println("update vertex of KeyGroupStreamPartitioner");
						KeyGroupStreamPartitioner keyGroupStreamPartitioner = (KeyGroupStreamPartitioner) oldPartitioner;

						newPartitioner = new AssignedKeyGroupStreamPartitioner(
							keyGroupStreamPartitioner.getKeySelector(),
							keyGroupStreamPartitioner.getMaxParallelism(),
							partitionAssignment);
					} else if (oldPartitioner instanceof AssignedKeyGroupStreamPartitioner && partitionAssignment != null) {
						System.out.println("update vertex of AssignedKeyGroupStreamPartitioner");

						((AssignedKeyGroupStreamPartitioner) oldPartitioner).updateNewPartitionAssignment(partitionAssignment);
						newPartitioner = oldPartitioner;
					}
					// TODO scaling: StreamEdge.edgeId contains partitioner string, update it?
					// TODO scaling: what if RescalePartitioner

					if (newPartitioner != null) {
						jobEdge.setDistributionPattern(DistributionPattern.ALL_TO_ALL);
						jobEdge.setShipStrategyName(newPartitioner.toString());

						System.out.println(outEdge.getEdgeId());

						outEdge.setPartitioner(newPartitioner);
						inEdge.setPartitioner(newPartitioner);

						updatedEdges.put(inEdge.getEdgeId(), inEdge);
					}
				}
			}
		}

		return updatedEdges;
	}

	private void updateAllOperatorsConfig(StreamConfig chainEntryPointConfig, Map<String, StreamEdge> updatedEdges) {
		updateOperatorConfig(chainEntryPointConfig, updatedEdges);

		Map<Integer, StreamConfig> chainedConfigs = chainEntryPointConfig.getTransitiveChainedTaskConfigs(userCodeLoader);
		for (Map.Entry<Integer, StreamConfig> entry : chainedConfigs.entrySet()) {
			updateOperatorConfig(entry.getValue(), updatedEdges);
		}
		chainEntryPointConfig.setTransitiveChainedTaskConfigs(chainedConfigs);
	}

	private void updateOperatorConfig(StreamConfig operatorConfig, Map<String, StreamEdge> updatedEdges) {
		List<StreamEdge> nonChainedOutputs = operatorConfig.getNonChainedOutputs(userCodeLoader);
		for (int i = 0; i < nonChainedOutputs.size(); i++) {
			StreamEdge edge = nonChainedOutputs.get(i);
			if (updatedEdges.containsKey(edge.getEdgeId())) {
				nonChainedOutputs.set(i, updatedEdges.get(edge.getEdgeId()));
			}
		}
		operatorConfig.setNonChainedOutputs(nonChainedOutputs);
	}
}
