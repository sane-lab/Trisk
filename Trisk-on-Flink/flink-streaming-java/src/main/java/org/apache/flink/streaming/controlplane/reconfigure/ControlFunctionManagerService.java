package org.apache.flink.streaming.controlplane.reconfigure;


import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlFunction;

public interface ControlFunctionManagerService {

	void registerFunction(ControlFunction function);

	void reconfigure(int operatorID, ControlFunction function);
}
