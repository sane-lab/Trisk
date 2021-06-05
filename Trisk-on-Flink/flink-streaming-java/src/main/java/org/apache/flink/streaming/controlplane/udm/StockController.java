package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StockController extends AbstractController {
	private final Object lock = new Object();
	private final Map<String, String> experimentConfig;

	public final static String TEST_OPERATOR_NAME = "trisk.reconfig.operator.name";


	public StockController(ReconfigurationExecutor reconfigurationExecutor, Configuration configuration) {
		super(reconfigurationExecutor);
		experimentConfig = configuration.toMap();
	}

	protected void generateTest() throws InterruptedException {
		String testOperatorName = experimentConfig.getOrDefault(TEST_OPERATOR_NAME, "filter");
		int testOpID = findOperatorByName(testOperatorName);
		// 5s
		Thread.sleep(5000);
		loadBalancingAll(testOpID);
		// 100s
		Thread.sleep(95000);
		scaleOutOne(testOpID);
		// 200s
		Thread.sleep(100000);
		scaleOutOne(testOpID);
		// 400s
		Thread.sleep(200000);
		scaleInOne(testOpID);
	}

	private void loadBalancingAll(int testingOpID) throws InterruptedException {
		ExecutionPlan executionPlan = getReconfigurationExecutor().getTrisk();
		scalingByParallelism(testingOpID, executionPlan.getParallelism(testingOpID));
	}

	private void scaleOutOne(int testingOpID) throws InterruptedException {
		ExecutionPlan executionPlan = getReconfigurationExecutor().getTrisk();
		scalingByParallelism(testingOpID, executionPlan.getParallelism(testingOpID) + 1);
	}

	private void scaleInOne(int testingOpID) throws InterruptedException {
		ExecutionPlan executionPlan = getReconfigurationExecutor().getTrisk();
		scalingByParallelism(testingOpID, executionPlan.getParallelism(testingOpID) - 1);
	}

	private void scalingByParallelism(int testingOpID, int newParallelism) throws InterruptedException {
		System.out.println("++++++ start scaling");
		ExecutionPlan executionPlan = getReconfigurationExecutor().getTrisk();

		Map<Integer, List<Integer>> curKeyStateDistribution = executionPlan.getKeyStateDistribution(testingOpID);
		int oldParallelism = executionPlan.getParallelism(testingOpID);
		assert oldParallelism == curKeyStateDistribution.size() : "old parallelism does not match the key set";

		Map<Integer, List<Integer>> keyStateDistribution = preparePartitionAssignment(newParallelism);
		int maxParallelism = 128;
		for (int i = 0; i < maxParallelism; i++) {
			keyStateDistribution.get(i%newParallelism).add(i);
		}
		// update the parallelism
		executionPlan.setParallelism(testingOpID, newParallelism);

		if (oldParallelism == newParallelism) {
//			getReconfigurationExecutor().rebalance(executionPlan, testingOpID, this);
			loadBalancing(testingOpID, keyStateDistribution);
		} else {
//			getReconfigurationExecutor().rescale(executionPlan, testingOpID, isScaleIn, this);
			scaling(testingOpID, keyStateDistribution, null);
		}
	}

	private Map<Integer, List<Integer>> preparePartitionAssignment(int parallleism) {
		Map<Integer, List<Integer>> newKeyStateAllocation = new HashMap<>();
		for (int i = 0; i < parallleism; i++) {
			newKeyStateAllocation.put(i, new ArrayList<>());
		}
		return newKeyStateAllocation;
	}

	@Override
	public void defineControlAction() {
		// the testing jobGraph (workload) is in TestingWorkload.java, see that file to know how to use it.
		try {
			Thread.sleep(5000);
			generateTest();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
