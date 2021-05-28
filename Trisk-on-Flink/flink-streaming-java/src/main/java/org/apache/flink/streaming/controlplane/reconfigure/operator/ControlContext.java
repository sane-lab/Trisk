package org.apache.flink.streaming.controlplane.reconfigure.operator;

public class ControlContext {

	private Object currentRes;

    public <T> T emitResult() {
    	T res = (T) currentRes;
    	this.currentRes = null;
    	return res;
    }

	public void setCurrentRes(Object currentRes) {
		this.currentRes = currentRes;
	}
}
