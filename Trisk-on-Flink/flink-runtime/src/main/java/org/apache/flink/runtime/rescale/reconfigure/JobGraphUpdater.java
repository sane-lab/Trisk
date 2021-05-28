package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.util.Map;

public interface JobGraphUpdater extends JobGraphRescaler {

	/**
	 *
	 * @param operatorID
	 * @param <OUT>
	 * @return
	 * @throws Exception
	 */
	<OUT> JobVertexID updateOperator(int operatorID, OperatorDescriptor.ExecutionLogic executionLogic) throws Exception;

	Map<Integer, OperatorID> getOperatorIDMap();
}
