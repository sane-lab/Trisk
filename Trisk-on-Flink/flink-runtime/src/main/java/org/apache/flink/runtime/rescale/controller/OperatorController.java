package org.apache.flink.runtime.rescale.controller;

import java.util.List;

public interface OperatorController {

	void init(OperatorControllerListener listener, List<String> executors, List<String> partitions);

	void start();

	void stopGracefully();

	//Method used to inform Controller
	void onMigrationExecutorsStopped();
	void onMigrationCompleted();
}
