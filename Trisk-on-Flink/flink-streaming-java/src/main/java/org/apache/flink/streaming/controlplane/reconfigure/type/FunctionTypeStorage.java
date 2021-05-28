package org.apache.flink.streaming.controlplane.reconfigure.type;

import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlFunction;

/**
 * Only the JVM could found the corresponding function type coudl we update operator logic with this function.
 * Since some function type may not exist when submitting the job.
 */
public interface FunctionTypeStorage {

	void addFunctionType(Class<? extends ControlFunction> type);

	ControlFunction getTargetFunction(Class<? extends ControlFunction> type);

}
