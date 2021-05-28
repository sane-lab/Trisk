package org.apache.flink.streaming.controlplane.reconfigure.operator;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple factory for the Update Operator
 */
public class ControlOperatorFactory<IN, OUT> implements StreamOperatorFactory<OUT> {

	// use to check whether this operator update is valid
	OperatorIOTypeDescriptor descriptor;

	private ControlOperator<IN, OUT> operator = null;
	private final ControlFunction function;

	public ControlOperatorFactory(int operatorID, ControlFunction function) {
		// using default control function
		descriptor = new OperatorIOTypeDescriptor(operatorID);
		this.function = function;
	}

	protected ControlOperator<IN, OUT> create(ControlFunction function) {
		return new ControlOperator<>(function);
	}

	private StreamOperator<OUT> getOperator() {
		if (operator == null) {
			operator = create(this.function);
		}
		return checkNotNull(operator, "operator not set...");
	}

	@Override
	public <T extends StreamOperator<OUT>> T createStreamOperator(
		StreamTask<?, ?> containingTask,
		StreamConfig config,
		Output<StreamRecord<OUT>> output) {
		T operator = (T) getOperator();
		if (operator instanceof AbstractStreamOperator) {
			((AbstractStreamOperator) operator).setup(containingTask, config, output);
		}
		return operator;
	}

	@Override
	public void setChainingStrategy(ChainingStrategy strategy) {
		operator.setChainingStrategy(strategy);
	}

	@Override
	public ChainingStrategy getChainingStrategy() {
		return operator.getChainingStrategy();
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return operator.getClass();
	}

}
