package org.apache.flink.streaming.controlplane.jobgraph;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.controlplane.abstraction.*;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.SimpleUdfStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.TriskImpl;
import org.apache.flink.streaming.runtime.partitioner.AssignedKeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

public class ExecutionPlanBuilder {
	// operatorId -> operator
	private final Map<Integer, OperatorDescriptor> operatorsMap = new LinkedHashMap<>();
	private final OperatorDescriptor[] headOperators;
	// operatorId -> task
	private final Map<Integer, Map<Integer, TaskResourceDescriptor>> operatorToTaskMap = new HashMap<>();
	// node with resources
	private final List<NodeDescriptor> resourceDistribution;

	public ExecutionPlanBuilder(JobGraph jobGraph, ExecutionGraph executionGraph, ClassLoader userClassLoader) {
		Map<OperatorID, Integer> operatorIdToVertexId = new HashMap<>();
		for (JobVertex vertex : jobGraph.getVertices()) {
			StreamConfig streamConfig = new StreamConfig(vertex.getConfiguration());
			Map<Integer, StreamConfig> configMap = streamConfig.getTransitiveChainedTaskConfigsWithSelf(userClassLoader);
			for (StreamConfig config : configMap.values()) {
				operatorIdToVertexId.put(config.getOperatorID(), config.getVertexID());
			}
		}
		resourceDistribution = initDeploymentGraphState(executionGraph, operatorIdToVertexId);
		headOperators = initializeOperatorGraphState(jobGraph, userClassLoader);
	}


	public TriskImpl build() {
//		return new TriskImpl(operatorsMap, headOperators, operatorToTaskMap, resourceDistribution);
//		return new TriskImpl(operatorsMap, operatorToTaskMap, resourceDistribution);
		return new TriskImpl(operatorsMap, resourceDistribution);
	}

	// DeployGraphState related
	private List<NodeDescriptor> initDeploymentGraphState(ExecutionGraph executionGraph, Map<OperatorID, Integer> operatorIdToVertexId) {
		Map<ResourceID, NodeDescriptor> resources = new HashMap<>();

		for (ExecutionJobVertex jobVertex : executionGraph.getAllVertices().values()) {
			// contains all tasks of the same parallel operator instances
//			List<Task> taskList = new ArrayList<>(jobVertex.getParallelism());
			Map<Integer, TaskResourceDescriptor> taskMap = new HashMap<>();
			for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
				Execution execution;
				do {
					// TODO: special check for scale out, which must have null execution
					execution = vertex.getCurrentExecutionAttempt();
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} while (execution == null || execution.getState() != ExecutionState.RUNNING);
				LogicalSlot slot = execution.getAssignedResource();
				NodeDescriptor node = resources.get(slot.getTaskManagerLocation().getResourceID());
				if (node == null) {
					node = new NodeDescriptor(slot.getTaskManagerLocation().address(), 0);
					resources.put(slot.getTaskManagerLocation().getResourceID(), node);
				}
				// todo how to get number of slots?
				SlotID slotID = new SlotID(slot.getTaskManagerLocation().getResourceID(), slot.getPhysicalSlotNumber());
				TaskResourceDescriptor task = new TaskResourceDescriptor(slotID.toString(), node);
				taskMap.put(vertex.getParallelSubtaskIndex(), task);
			}
			for (OperatorID operatorID : jobVertex.getOperatorIDs()) {
				operatorToTaskMap.put(operatorIdToVertexId.get(operatorID), taskMap);
			}
		}
		return new ArrayList<>(resources.values());
	}

	// OperatorGraphState related
	/* contains topology of this stream job */
	private OperatorDescriptor[] initializeOperatorGraphState(JobGraph jobGraph, ClassLoader userCodeLoader) {
		// add all nodes
		Map<Integer, StreamConfig> streamConfigMap = new HashMap<>();
		for (JobVertex vertex : jobGraph.getVertices()) {
			StreamConfig streamConfig = new StreamConfig(vertex.getConfiguration());
			Map<Integer, StreamConfig> configMap = streamConfig.getTransitiveChainedTaskConfigsWithSelf(userCodeLoader);
			streamConfigMap.putAll(configMap);
			for (StreamConfig config : configMap.values()) {
				OperatorDescriptor operatorDescriptor = new OperatorDescriptor(
					config.getVertexID(),
					config.getOperatorName(),
					vertex.getParallelism(),
					operatorToTaskMap.get(config.getVertexID()));
				operatorsMap.put(config.getVertexID(), operatorDescriptor);
			}
		}
		// build topology
		for (OperatorDescriptor operatorDescriptor : operatorsMap.values()) {
			StreamConfig config = streamConfigMap.get(operatorDescriptor.getOperatorID());
			List<Tuple2<Integer, Integer>> outEdges = config.getOutEdges(userCodeLoader)
				.stream()
				.map(e -> Tuple2.of(e.getSourceId(), e.getTargetId()))
				.collect(Collectors.toList());
			OperatorDescriptorVisitor.attachOperator(operatorDescriptor).addChildren(outEdges, operatorsMap);

			List<Tuple2<Integer, Integer>> inEdges = config.getInPhysicalEdges(userCodeLoader)
				.stream()
				.map(e -> Tuple2.of(e.getSourceId(), e.getTargetId()))
				.collect(Collectors.toList());
			OperatorDescriptorVisitor.attachOperator(operatorDescriptor).addParent(inEdges, operatorsMap);
		}
		// find head
		List<OperatorDescriptor> heads = new ArrayList<>();
		for (OperatorDescriptor descriptor : operatorsMap.values()) {
			if (descriptor.getParents().isEmpty()) {
				heads.add(descriptor);
			}
		}
		checkRelationship(operatorsMap.values());
		// finished other field
		for (OperatorDescriptor descriptor : operatorsMap.values()) {
			StreamConfig streamConfig = streamConfigMap.get(descriptor.getOperatorID());
			// add workload dimension info
			Map<Integer, List<Integer>> keyStateAllocation = getKeyStateAllocation(streamConfig, userCodeLoader, descriptor.getParallelism());
			OperatorDescriptorVisitor.attachOperator(descriptor).setKeyStateAllocation(keyStateAllocation);
			// add execution logic info
			try {
				initializeFunction(descriptor, streamConfigMap.get(descriptor.getOperatorID()), userCodeLoader);
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		return heads.toArray(new OperatorDescriptor[0]);
	}

	private static void checkRelationship(Collection<OperatorDescriptor> allOp) {
		for (OperatorDescriptor descriptor : allOp) {
			for (OperatorDescriptor child : descriptor.getChildren()) {
				Preconditions.checkArgument(child.getParents().contains(descriptor), child + "'s parents should contain " + descriptor);
			}
			for (OperatorDescriptor parent : descriptor.getParents()) {
				Preconditions.checkArgument(parent.getChildren().contains(descriptor), parent + "'s children should contain " + descriptor);
			}
		}
	}

	private static Map<Integer, List<Integer>> getKeyStateAllocation(StreamConfig config, ClassLoader userCodeLoader, int parallelism) {
		Map<Integer, Map<Integer, List<Integer>>> res = new HashMap<>();
		// very strange that some operator's out physical edge may not be completed
		List<StreamEdge> inPhysicalEdges = config.getInPhysicalEdges(userCodeLoader);
		for (StreamEdge edge : inPhysicalEdges) {
			res.put(edge.getSourceId(), getKeyMessage(edge, parallelism));
		}
		if (res.isEmpty()) {
//			throw new UnsupportedOperationException(
//				"Non-keyed stream is not supported, please use keyBy before operating on streams.");
			return new HashMap<>();
		} else {
			// todo, check all key set is same
			return res.values().iterator().next();
		}
	}

	private static Map<Integer, List<Integer>> getKeyMessage(StreamEdge streamEdge, int parallelism) {
		StreamPartitioner<?> partitioner = streamEdge.getPartitioner();
		if (partitioner instanceof AssignedKeyGroupStreamPartitioner) {
			return ((AssignedKeyGroupStreamPartitioner<?, ?>) partitioner).getKeyMappingInfo(parallelism);
		} else if (partitioner instanceof KeyGroupStreamPartitioner) {
			return ((KeyGroupStreamPartitioner<?, ?>) partitioner).getKeyMappingInfo(parallelism);
		}
		// the consumer operator may be not a key stream operator
		// We do not need to consider non-keyed stream, because it is hard to operate and manage and also out of our scope.
//		throw new UnsupportedOperationException(
//			"Non-keyed stream is not supported, please use keyBy before operating on streams.");
		return new HashMap<>();
	}

	private static void initializeFunction(
		OperatorDescriptor descriptor,
		StreamConfig config,
		ClassLoader userCodeLoader) throws IllegalAccessException {

		StreamOperatorFactory<?> factory = config.getStreamOperatorFactory(userCodeLoader);
		if (factory instanceof SimpleUdfStreamOperatorFactory) {
			Function function = ((SimpleUdfStreamOperatorFactory<?>) factory).getUserFunction();
			descriptor.setUdf(function);
			StreamOperator<?> streamOperator = ((SimpleUdfStreamOperatorFactory<?>) factory).getOperator();
			Class<?> streamOperatorClass = streamOperator.getClass();
			List<Field> fieldList = new LinkedList<>();
			do {
				fieldList.addAll(
					Arrays.stream(streamOperatorClass.getDeclaredFields())
						.filter(field -> field.isAnnotationPresent(ControlAttribute.class))
						.collect(Collectors.toList())
				);
				streamOperatorClass = streamOperatorClass.getSuperclass();
			} while (streamOperatorClass != null);
			OperatorDescriptorVisitor.attachOperator(descriptor).setAttributeField(streamOperator, fieldList);
		}
	}
}

