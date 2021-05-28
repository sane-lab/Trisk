package org.apache.flink.streaming.controlplane.jobgraph;

import org.apache.flink.runtime.controlplane.ExecutionPlanAndJobGraphUpdaterFactory;
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rescale.reconfigure.JobGraphUpdater;

public enum DefaultExecutionPlanAndJobGraphUpdaterFactory implements ExecutionPlanAndJobGraphUpdaterFactory {
	INSTANCE;

	@Override
	public ExecutionPlan createExecutionPlan(JobGraph jobGraph, ExecutionGraph executionGraph, ClassLoader userClassLoader) {
		return new ExecutionPlanBuilder(jobGraph, executionGraph, userClassLoader).build();
	}

	@Override
	public JobGraphUpdater createJobGraphUpdater(JobGraph jobGraph, ClassLoader classLoader) {
		return new StreamJobGraphUpdater(jobGraph, classLoader);
	}
}
