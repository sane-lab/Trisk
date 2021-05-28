package org.apache.flink.streaming.controlplane.reconfigure.type;

import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlFunction;

public class InMemoryFunctionStorge implements FunctionTypeStorage{
	@Override
	public void addFunctionType(Class<? extends ControlFunction> type) {

	}

	@Override
	public ControlFunction getTargetFunction(Class<? extends ControlFunction> type) {
		return null;
	}
}
