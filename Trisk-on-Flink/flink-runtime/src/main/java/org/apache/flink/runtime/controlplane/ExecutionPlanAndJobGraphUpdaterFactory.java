package org.apache.flink.runtime.controlplane;

import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rescale.reconfigure.JobGraphUpdater;

public interface ExecutionPlanAndJobGraphUpdaterFactory {

	ExecutionPlan createExecutionPlan(
		JobGraph jobGraph,
		ExecutionGraph executionGraph,
		ClassLoader classLoader);

	JobGraphUpdater createJobGraphUpdater(
		JobGraph jobGraph,
		ClassLoader classLoader);
}
