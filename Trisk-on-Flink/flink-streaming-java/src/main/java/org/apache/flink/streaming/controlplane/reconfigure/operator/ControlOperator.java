package org.apache.flink.streaming.controlplane.reconfigure.operator;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

public class ControlOperator<IN, OUT> extends AbstractUdfStreamOperator<OUT, ControlFunction>
	implements OneInputStreamOperator<IN, OUT> {

	private transient ControlContext controlContext;

	public ControlOperator(ControlFunction userFunction) {
		super(userFunction);
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);
		controlContext = new ControlContext();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		this.userFunction.invokeControl(controlContext, element.getValue());
		OUT o = controlContext.emitResult();
		if (null != o) {
			super.output.collect(new StreamRecord<>(o));
		}
	}
}
