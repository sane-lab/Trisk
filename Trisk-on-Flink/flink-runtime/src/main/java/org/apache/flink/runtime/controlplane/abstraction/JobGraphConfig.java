package org.apache.flink.runtime.controlplane.abstraction;

import org.apache.flink.api.common.functions.Function;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface JobGraphConfig {
	/**
	 * Return UserFunction, StreamOperator, or StreamOperatorFactory?
	 * The Function type belongs to Flink, should we use a new data structure since we want to decouple
	 *
	 * @param operatorID the operator id of this operator
	 * @return
	 */
	Function getUserFunction(Integer operatorID);

	/**
	 * To get how the input key state was allocated among the sub operator instance.
	 * Map(sourceVertexId -> [operator index, [assigned keys]])
	 *
	 * Multiple sourceVertexId means there are multiple input edges.
	 *
	 * @param operatorID the target key mapping of operator we want to know
	 * @return A map from streamEdgeId and the key mapping of this stream edge, the map value is a list o list
	 * representing the output channel with its assigned keys.
	 * @throws Exception
	 */
	Map<Integer, List<Integer>> getKeyStateAllocation(Integer operatorID);

	/**
	 * Return how the key mapping to down stream operator.
	 * Map(targetVertexId -> [target operator index, [assigned keys]])
	 *
	 * Multiple targetVertex means there are multiple output edges.
	 *
	 * @param operatorID the target key mapping of operator we want to know
	 * @return A map from streamEdgeId and the key mapping of this stream edge, the map value is a list o list
	 * representing the output channel with its assigned keys.
	 * @throws Exception
	 */
	Map<Integer, Map<Integer, List<Integer>>> getKeyMapping(Integer operatorID);

	/**
	 * Return the parallelism of operator with given operator id
	 * @param operatorID
	 * @return
	 */
	int getParallelism(Integer operatorID);

	Iterator<OperatorDescriptor> getAllOperatorDescriptor();

	OperatorDescriptor getOperatorDescriptorByID(Integer operatorID);
}
