package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.TaskDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.resource.AbstractSlot;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ExecutionPlanWithLock;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
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
	public void startControllers() {
		super.controlActionRunner.start();
	}

	@Override
	public void stopControllers() {
		super.controlActionRunner.interrupt();
	}

	@Override
	protected void defineControlAction() throws Exception {
		super.defineControlAction();
		ExecutionPlanWithLock planWithLock;

		requestTime = 0;
		planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
		updateDecisionTreeParameter(planWithLock);
		Thread.sleep(10 * 1000);

		Thread.sleep(2 * 60 * 1000);
		requestTime = 120;
		planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
		updateDecisionTreeParameter(planWithLock);

		Thread.sleep(10 * 1000);
		planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
		int dtreeOpID = findOperatorByName("dtree");
		smartPlacementV2(dtreeOpID, planWithLock.getParallelism(dtreeOpID) - 4);

		Thread.sleep(55 * 1000);
		requestTime = 225;
		planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
		updateDecisionTreeParameter(planWithLock);

		Thread.sleep(120 * 1000);
		requestTime = 345;
		planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();
		updateDecisionTreeParameter(planWithLock);
	}

	private void smartPlacement(int preprocessOpID) throws Exception {
		ExecutionPlanWithLock planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();

		Map<Integer, Tuple2<Integer, String>> deployment = new HashMap<>();

		Map<String, List<AbstractSlot>> resourceMap = planWithLock.getResourceDistribution();
		int p = planWithLock.getParallelism(preprocessOpID);
		List<AbstractSlot> allocatedSlots = allocateResourceUniformly(resourceMap, p);
		assert allocatedSlots != null;
		for (int i = 0; i < p; i++) {
			deployment.put(i, Tuple2.of(i + p, allocatedSlots.get(i).getId()));
		}
		placement(preprocessOpID, deployment);
	}

	private List<AbstractSlot> allocateResourceUniformly(Map<String, List<AbstractSlot>> resourceMap, int numTasks) throws Exception {
		List<AbstractSlot> res = new ArrayList<>(numTasks);
		int numNodes = resourceMap.size();
		// todo, please ensure numTask could be divided by numNodes for experiment
		if (numTasks % numNodes != 0) {
			throw new Exception("please ensure numTask could be divided by numNodes for experiment");
		}
		int numTasksInOneNode = numTasks / numNodes;

		for (String nodeID : resourceMap.keySet()) {
			List<AbstractSlot> slotList = resourceMap.get(nodeID);
			int allocated = 0;
			for (AbstractSlot slot : slotList) {
				if (allocated >= numTasksInOneNode) {
					break;
				}
				if (slot.getState() == AbstractSlot.State.FREE) {
					System.out.println("++++++ choosing slot: " + slot);
					res.add(slot);
					allocated++;
				}
			}
		}
		if (res.size() == numTasks) {
			// remove them from source map
			for (AbstractSlot slot : res) {
				resourceMap.get(slot.getLocation()).remove(slot);
			}
			return res;
		} else {
			return null;
		}
	}

	private void smartPlacementV2(int testOpID, int maxTaskOneNode) throws Exception {
		ExecutionPlanWithLock planWithLock = getReconfigurationExecutor().getExecutionPlanCopy();

		Map<Integer, String> deployment = new HashMap<>();

		Map<String, List<AbstractSlot>> resourceMap = planWithLock.getResourceDistribution();

		OperatorDescriptor operatorDescriptor = planWithLock.getOperatorByID(testOpID);

		int p = planWithLock.getParallelism(testOpID);
		Map<String, AbstractSlot> allocatedSlots = allocateResourceUniformlyV2(resourceMap, p, maxTaskOneNode);
		Preconditions.checkNotNull(allocatedSlots, "no more slots can be allocated");
		// place half of tasks with new slots
		List<Integer> modifiedTasks = new ArrayList<>();
		for (int taskId = 0; taskId < p; taskId++) {
			TaskDescriptor task = operatorDescriptor.getTask(taskId);
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

//		placement(testOpID, deployment);


		placementV2(testOpID, deployment);
	}


	private Map<String, AbstractSlot> allocateResourceUniformlyV2(Map<String, List<AbstractSlot>> resourceMap, int numTasks, int maxTaskOneNode) throws Exception {
		// slotId to slot mapping
		Map<String, AbstractSlot> res = new HashMap<>(numTasks);
		int numNodes = resourceMap.size();
		// todo, please ensure numTask could be divided by numNodes for experiment
		if (numTasks % numNodes != 0) {
			throw new Exception("please ensure numTask could be divided by numNodes for experiment");
		}
		int numTasksInOneNode = maxTaskOneNode;
		System.out.println("++++++ number of tasks on each nodes: " + numTasksInOneNode);

		HashMap<String, Integer> loadMap = new HashMap<>();
		HashMap<String, Integer> pendingStots = new HashMap<>();
		HashMap<String, Integer> releasingStots = new HashMap<>();
		// compute the num of tasks in each node
		for (String nodeID : resourceMap.keySet()) {
			List<AbstractSlot> slotList = resourceMap.get(nodeID);
			for (AbstractSlot slot : slotList) {
				if (slot.getState() == AbstractSlot.State.ALLOCATED) {
					loadMap.put(nodeID, loadMap.getOrDefault(nodeID, 0) + 1);
					if (loadMap.getOrDefault(nodeID, 0) <= numTasksInOneNode) {
						res.put(slot.getId(), slot);
					}
				}
			}
		}

		// try to migrate uniformly
		for (String nodeID : resourceMap.keySet()) {
			// the node is overloaded, free future slots, and allocate a new slot in other nodes
			if (loadMap.getOrDefault(nodeID, 0) > numTasksInOneNode) {
				int nReleasingSlots = loadMap.getOrDefault(nodeID, 0) - numTasksInOneNode;
				releasingStots.put(nodeID, releasingStots.getOrDefault(nodeID, 0) + nReleasingSlots);
				for (int i = 0; i < nReleasingSlots; i++) {
					findUnusedSlot(numTasksInOneNode, loadMap, pendingStots, nodeID, resourceMap);
				}
			}
		}

		System.out.println("++++++ load map: " + loadMap);

		// free slots from heavy nodes, and allocate slots in light nodes
		for (String nodeID : resourceMap.keySet()) {
			allocateSlotsOnOneNode(resourceMap, res, pendingStots, nodeID);
		}
		if (res.size() == numTasks) {
			// remove them from source map
			// TODO: slot should be marked as ALLOCATED and unused slots should be marked as FREE.
//			for (AbstractSlot slot : res.values()) {
//				resourceMap.get(slot.getLocation()).remove(slot);
//			}
			return res;
		} else {
			return null;
		}
	}

	private void allocateSlotsOnOneNode(Map<String, List<AbstractSlot>> resourceMap, Map<String, AbstractSlot> res, HashMap<String, Integer> pendingStots, String nodeID) {
		List<AbstractSlot> slotList = resourceMap.get(nodeID);
		int allocated = 0;
		for (AbstractSlot slot : slotList) {
			if (allocated >= pendingStots.getOrDefault(nodeID, 0)) {
				continue;
			}
			if (slot.getState() == AbstractSlot.State.FREE) {
				System.out.println("++++++ choosing slot: " + slot);
				res.put(slot.getId(), slot);
				allocated++;
			}
		}
	}

	private void findUnusedSlot(int numTasksInOneNode, HashMap<String, Integer> loadMap,
								HashMap<String, Integer> pendingStots, String nodeID,
								Map<String, List<AbstractSlot>> resourceMap) {
		for (String otherNodeID : resourceMap.keySet()) {
			if (loadMap.getOrDefault(otherNodeID, 0) < numTasksInOneNode) {
				System.out.println("++++++ exceeded number of tasks on node: " + nodeID
					+ " allocate exceeded one to another node: " + otherNodeID);
				pendingStots.put(otherNodeID, pendingStots.getOrDefault(otherNodeID, 0) + 1);
				loadMap.put(otherNodeID, loadMap.getOrDefault(otherNodeID, 0) + 1);
				break;
			}
		}
	}


	private void updatePreprocessingScaleParameter(ExecutionPlanWithLock planWithLock) throws Exception {
		String scalePara = sendGet("http://127.0.0.1:5000/scale/", requestTime);
		Map<String, Object> res = parseJsonString(scalePara);
		ArrayList<Double> center = (ArrayList<Double>) res.get("center");
		ArrayList<Double> scale = (ArrayList<Double>) res.get("scale");

		int preprocessOpID = findOperatorByName("preprocess");
		// preprocessing
		float[] centerArray = doubleListToArray(center);
		float[] scaleArray = doubleListToArray(scale);
		Function preprocessFunc = planWithLock.getOperatorByID(preprocessOpID).getUdf();
		Function newPreprocessFunc = preprocessFunc.getClass()
			.getConstructor(float[].class, float[].class).newInstance(centerArray, scaleArray); // reflection related
		changeOfLogic(preprocessOpID, newPreprocessFunc);
	}

	private float[] doubleListToArray(ArrayList<Double> doubles) {
		float[] floats = new float[doubles.size()];
		for (int i = 0; i < doubles.size(); i++) {
			floats[i] = doubles.get(i).floatValue();
		}
		return floats;
	}

	private void updateDecisionTreeParameter(ExecutionPlanWithLock planWithLock) throws Exception {
		// preprocessing
		String scalePara = sendGet("http://127.0.0.1:5000/scale/", requestTime);
		Map<String, Object> scaleRes = parseJsonString(scalePara);
		ArrayList<Double> center = (ArrayList<Double>) scaleRes.get("center");
		ArrayList<Double> scale = (ArrayList<Double>) scaleRes.get("scale");
		float[] centerArray = doubleListToArray(center);
		float[] scaleArray = doubleListToArray(scale);
		// processing
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
		// processing
		Function processFunc = planWithLock.getOperatorByID(processOpID).getUdf();
		Class<?> decisionTreeRuleClass = processFunc.getClass().getClassLoader().loadClass("flinkapp.frauddetection.rule.DecisionTreeRule");
		Object newRule = decisionTreeRuleClass        // reflection related
			.getConstructor(int[].class, float[].class, int[].class, int[].class, float[][].class)
			.newInstance(featureArr, thresholdArr, leftArr, rightArr, valueArr);
		Function newProcessFunc = processFunc.getClass()
			.getConstructor(decisionTreeRuleClass.getSuperclass(), float[].class, float[].class)
			.newInstance(newRule, centerArray, scaleArray);
		changeOfLogic(processOpID, newProcessFunc);
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
