package org.apache.flink.streaming.controlplane.reconfigure;

import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.streaming.controlplane.reconfigure.operator.ControlFunction;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.apache.flink.streaming.controlplane.udm.ControlPolicy;

public class TestingCFManager extends ControlFunctionManager implements ControlPolicy {


	public TestingCFManager(ReconfigurationExecutor reconfigurationExecutor) {
		super(reconfigurationExecutor);
	}

	@Override
	public void startControllerInternal() {
		System.out.println("Testing Control Function Manager starting...");

		ExecutionPlan jobState = getReconfigurationExecutor().getExecutionPlan();

		int secondOperatorId = findOperatorByName("filte");

		if(secondOperatorId != -1) {
			asyncRunAfter(5, () -> this.getKeyStateMapping(
				findOperatorByName("Splitter")));
			asyncRunAfter(5, () -> this.getKeyStateAllocation(
				findOperatorByName("filte")));
			asyncRunAfter(10, () -> this.reconfigure(secondOperatorId, getFilterFunction(2)));
		}
	}

	private void getKeyStateMapping(int operatorID){
		try {
			this.getReconfigurationExecutor().getExecutionPlan().getKeyMapping(operatorID);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	private void getKeyStateAllocation(int operatorID){
		try {
			this.getReconfigurationExecutor().getExecutionPlan().getKeyStateAllocation(operatorID);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static ControlFunction getFilterFunction(int k) {
		return (ControlFunction) (ctx, input) -> {
			String inputWord = (String) input;
			System.out.println("now filter the words that has length smaller than " + k);
			if (inputWord.length() >= k) {
				ctx.setCurrentRes(inputWord);
			}
		};
	}


	private void asyncRunAfter(int seconds, Runnable runnable) {
		new Thread(() -> {
			try {
				Thread.sleep(seconds * 1000);
				runnable.run();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}).start();
	}

	@Override
	public void startControllers() {
		this.startControllerInternal();
	}

	@Override
	public void stopControllers() {
		System.out.println("Testing Control Function Manager stopping...");
	}

	@Override
	public void onChangeCompleted(Throwable throwable) {
		if(throwable != null) {
			System.out.println(System.currentTimeMillis() + ":one operator function update is finished");
		}
	}

}
