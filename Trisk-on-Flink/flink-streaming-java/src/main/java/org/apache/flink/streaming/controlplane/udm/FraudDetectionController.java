package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.TaskResourceDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.resource.AbstractSlot;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.TriskWithLock;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class FraudDetectionController extends AbstractController {

	private final String USER_AGENT = "Mozilla/5.0";
	private int requestTime = 0;
	private String baseDir;

	public FraudDetectionController(ReconfigurationExecutor reconfigurationExecutor) {
		super(reconfigurationExecutor);
		baseDir = reconfigurationExecutor.getExperimentConfig().getString("trisk.exp.dir", "/data/flink");
	}

	@Override
	protected void defineControlAction() throws Exception {
		super.defineControlAction();
		TriskWithLock planWithLock;

		requestTime = 0;
		submitNewFunction();

//		planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
//		updateDecisionTreeParameter(planWithLock);
//		Thread.sleep(10 * 1000);
//
//		Thread.sleep(2 * 60 * 1000);
//		requestTime = 120;
//		planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
//		updateDecisionTreeParameter(planWithLock);
//
//		Thread.sleep(10 * 1000);
//		planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
//		int dtreeOpID = findOperatorByName("dtree");
//		smartPlacementV2(dtreeOpID, planWithLock.getParallelism(dtreeOpID) - 4);
//
//		Thread.sleep(55 * 1000);
//		requestTime = 225;
//		planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
//		updateDecisionTreeParameter(planWithLock);
//
//		Thread.sleep(120 * 1000);
//		requestTime = 345;
//		planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
//		updateDecisionTreeParameter(planWithLock);
	}

	private void smartPlacementV2(int testOpID, int maxTaskOneNode) throws Exception {
		TriskWithLock planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();

		Map<Integer, String> deployment = new HashMap<>();

		Map<String, List<AbstractSlot>> resourceMap = planWithLock.getResourceDistribution();

		OperatorDescriptor operatorDescriptor = planWithLock.getOperatorByID(testOpID);

		int p = planWithLock.getParallelism(testOpID);
		Map<String, AbstractSlot> allocatedSlots = allocateResourceUniformlyV2(resourceMap, p, maxTaskOneNode);
		Preconditions.checkNotNull(allocatedSlots, "no more slots can be allocated");
		// place half of tasks with new slots
		List<Integer> modifiedTasks = new ArrayList<>();
		for (int taskId = 0; taskId < p; taskId++) {
			TaskResourceDescriptor task = operatorDescriptor.getTaskResource(taskId);
			// if the task slot is in the allocated slot, this task is unmodified
			if (allocatedSlots.containsKey(task.resourceSlot)) {
				deployment.put(taskId, task.resourceSlot);
				allocatedSlots.remove(task.resourceSlot);
			} else {
				modifiedTasks.add(taskId);
			}
		}

		Preconditions.checkState(modifiedTasks.size() == allocatedSlots.size(),
			"inconsistent task to new slots allocation");

		List<AbstractSlot> allocatedSlotsList = new ArrayList<>(allocatedSlots.values());

		for (int i = 0; i < modifiedTasks.size(); i++) {
			int taskId = modifiedTasks.get(i);
			int newTaskId = taskId + p;
			deployment.put(taskId, allocatedSlotsList.get(i).getId());
		}
		placementV2(testOpID, deployment);
	}

	private float[] doubleListToArray(ArrayList<Double> doubles) {
		float[] floats = new float[doubles.size()];
		for (int i = 0; i < doubles.size(); i++) {
			floats[i] = doubles.get(i).floatValue();
		}
		return floats;
	}

	private void updateDecisionTreeParameter(TriskWithLock planWithLock) throws Exception {
		// preprocessing parameter
		String scalePara = sendGet("http://127.0.0.1:5000/scale/", requestTime);
		Map<String, Object> scaleRes = parseJsonString(scalePara);
		ArrayList<Double> center = (ArrayList<Double>) scaleRes.get("center");
		ArrayList<Double> scale = (ArrayList<Double>) scaleRes.get("scale");
		float[] centerArray = doubleListToArray(center);
		float[] scaleArray = doubleListToArray(scale);
		// processing parameter
		String treePara = sendGet("http://127.0.0.1:5000/dtree/", requestTime);
		Map<String, Object> res = parseJsonString(treePara);
		ArrayList<Integer> feature = (ArrayList<Integer>) res.get("feature");
		ArrayList<Integer> leftChildren = (ArrayList<Integer>) res.get("children_left");
		ArrayList<Integer> rightChildren = (ArrayList<Integer>) res.get("children_right");
		ArrayList<Double> threshold = (ArrayList<Double>) res.get("threshold");
		ArrayList<ArrayList<?>> value = (ArrayList<ArrayList<?>>) res.get("value");
		int[] featureArr = feature.stream().mapToInt(i -> i).toArray();
		float[] thresholdArr = doubleListToArray(threshold);
		int[] leftArr = leftChildren.stream().mapToInt(i -> i).toArray();
		int[] rightArr = rightChildren.stream().mapToInt(i -> i).toArray();
		float[][] valueArr = new float[value.size()][2];
		for (int i = 0; i < value.size(); i++) {
			ArrayList<Double> possibility = (ArrayList<Double>) value.get(i).get(0);
			valueArr[i][0] = possibility.get(0).floatValue();
			valueArr[i][1] = (possibility.get(1)).floatValue();
		}
		int processOpID = findOperatorByName("dtree");
		// update processing operator
		Function processFunc = planWithLock.getOperatorByID(processOpID).getUdf();
		Object newRule = updateToNewClass(
			"flinkapp.frauddetection.rule.DecisionTreeRule",
			new Class[]{int[].class, float[].class, int[].class, int[].class, float[][].class},
			featureArr, thresholdArr, leftArr, rightArr, valueArr);
		Function newProcessFunc = updateConstructorParameter(
			processFunc.getClass(),
			new Class[]{newRule.getClass().getSuperclass(), float[].class, float[].class},
			newRule, centerArray, scaleArray
		);
		changeOfLogic(processOpID, newProcessFunc);
	}

	private void submitNewFunction() throws InterruptedException {
		String sourceCode = "package flinkapp.frauddetection.function;\n" +
			"\n" +
			"import flinkapp.frauddetection.rule.FraudOrNot;\n" +
			"import flinkapp.frauddetection.rule.NoRule;\n" +
			"import flinkapp.frauddetection.rule.Rule;\n" +
			"import flinkapp.frauddetection.transaction.PrecessedTransaction;\n" +
			"import flinkapp.frauddetection.transaction.Transaction;\n" +
			"import org.apache.flink.streaming.api.functions.KeyedProcessFunction;\n" +
			"import org.apache.flink.util.Collector;" +
			"public class ProcessingFunctionV2 extends KeyedProcessFunction<String, Transaction, FraudOrNot> {\n" +
			"        \n" +
			"        float[] center;\n" +
			"        float[] scale;\n" +
			"\n" +
			"        public ProcessingFunctionV2() {\n" +
			"        }\n" +
			"\n" +
			"        public ProcessingFunctionV2(float[] center, float[] scale) {\n" +
			"            this.center = center;\n" +
			"            this.scale = scale;\n" +
			"        }\n" +
			"        \n" +
			"        @Override\n" +
			"        public void processElement(Transaction value, Context ctx, Collector<FraudOrNot> out) throws Exception {\n" +
			"            PrecessedTransaction precessedTransaction = new PrecessedTransaction(value, center, scale);\n" +
			"            System.out.println(\"use default rule: no rule\");\n" +
			"            out.collect(NoRule.getINSTANCE().isFraud(precessedTransaction));\n" +
			"        }\n" +
			"\n" +
			"    }";
		String className = "flinkapp.frauddetection.function.ProcessingFunctionV2";
		Function newFuncObj = (Function) defineNewClass(className, sourceCode, null);
		int processOpID = findOperatorByName("dtree");
		changeOfLogic(processOpID, newFuncObj);
	}

	private String sendGet(String url) throws Exception {
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		con.setRequestMethod("GET");

		con.setRequestProperty("User-Agent", USER_AGENT);

		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);

		BufferedReader in = new BufferedReader(
			new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		return response.toString();
	}

	private String sendGet(String url, int requestTime) throws Exception {
		System.out.println("\naccess from file for url: " + url + requestTime);
		String fileNamePrefix = url.contains("dtree") ? "dtree" : "scale";
		Path parameterFile = Paths.get(baseDir, fileNamePrefix + requestTime);
		return FileUtils.readFileUtf8(parameterFile.toFile());
	}
}
