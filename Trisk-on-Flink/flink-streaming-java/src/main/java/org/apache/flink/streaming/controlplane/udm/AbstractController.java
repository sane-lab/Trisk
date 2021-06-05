package org.apache.flink.streaming.controlplane.udm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.resource.AbstractSlot;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.controlplane.streammanager.ByteClassLoader;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.TriskWithLock;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * If you wish to submit your controller via restful API,
 * please insure the subclass of AbstractController do not have any inner class (lambda, anonymous inner class etc) !!!
 *
 */
public abstract class AbstractController implements ControlPolicy {

	private final ReconfigurationExecutor reconfigurationExecutor;
	private final Object lock = new Object();

	protected ControlActionRunner controlActionRunner = new ControlActionRunner();

	@Override
	public void onChangeStarted() throws InterruptedException {
		// wait for operation completed
		synchronized (lock) {
			lock.wait();
		}
	}

	@Override
	public synchronized void onChangeCompleted(Throwable throwable) {
		if(throwable != null){
			throw new RuntimeException("error while execute reconfiguration", throwable);
		}
		System.out.println("my self defined instruction finished??");
		synchronized (lock) {
			lock.notify();
		}
	}

	@Override
	public void startControllers() {
		this.controlActionRunner.start();
	}

	@Override
	public void stopControllers() {
		this.controlActionRunner.interrupt();
	}

	protected AbstractController(ReconfigurationExecutor reconfigurationExecutor){
		this.reconfigurationExecutor = reconfigurationExecutor;
	}

	public ReconfigurationExecutor getReconfigurationExecutor() {
		return reconfigurationExecutor;
	}

	protected int findOperatorByName(@Nonnull String name) {
		for (Iterator<OperatorDescriptor> it = reconfigurationExecutor.getTrisk().getAllOperator(); it.hasNext(); ) {
			OperatorDescriptor descriptor = it.next();
			if(descriptor.getName().equals(name)){
				return descriptor.getOperatorID();
			}
		}
		return -1;
	}

	protected void loadBalancing(int operatorId, Map<Integer, List<Integer>> newKeyDistribution) throws InterruptedException {
		TriskWithLock executionPlan = getReconfigurationExecutor().getExecutionPlanCopy();
		executionPlan
			.assignWorkload(operatorId, newKeyDistribution);
		getReconfigurationExecutor().execute(this, executionPlan);
		onChangeStarted();
	}

	protected void scaling(int operatorId, Map<Integer, List<Integer>> newKeyDistribution,
						   @Nullable Map<Integer, Tuple2<Integer, String>> deployment) throws InterruptedException {
		TriskWithLock executionPlan = getReconfigurationExecutor().getExecutionPlanCopy();
		executionPlan
			.assignWorkload(operatorId, newKeyDistribution)
			.assignResources(operatorId, deployment);
		getReconfigurationExecutor().execute(this, executionPlan);
		onChangeStarted();
	}

	protected void placement(Integer operatorId, @Nullable Map<Integer, Tuple2<Integer, String>> deployment) throws InterruptedException {
		TriskWithLock executionPlan = getReconfigurationExecutor().getExecutionPlanCopy();
		executionPlan
			.assignResources(operatorId, deployment);
		getReconfigurationExecutor().execute(this, executionPlan);
		onChangeStarted();
	}

	// this is the placement API we are actually using
	protected void placementV2(Integer operatorId, @Nullable Map<Integer, String> deployment) throws InterruptedException {
		TriskWithLock executionPlan = getReconfigurationExecutor().getExecutionPlanCopy();
		executionPlan
				.assignResourcesV2(operatorId, deployment);
		getReconfigurationExecutor().execute(this, executionPlan);
		onChangeStarted();
	}


	protected void changeOfLogic(Integer operatorId, Object function) throws InterruptedException {
		TriskWithLock executionPlan = getReconfigurationExecutor().getExecutionPlanCopy();
		executionPlan
			.assignExecutionLogic(operatorId, function);
		getReconfigurationExecutor().execute(this, executionPlan);
		onChangeStarted();
	}

	protected Map<String, Object> parseJsonString(String json) throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		return objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {
		});
	}

	/**
	 * All control action should be defined in this method
	 *
	 * @throws Exception
	 */
	protected void defineControlAction() throws Exception{}

	/* ===================== Placement utility method ========================== */

	/**
	 * return allocatable slots in this resource map by giving the limit of maxTask in one node
	 *
	 * @param resourceMap
	 * @param numTasks the number of slots task need to request
	 * @param maxTaskOneNode the max task could have in one node
	 * @return
	 * @throws Exception
	 */
	protected Map<String, AbstractSlot> allocateResourceUniformlyV2(Map<String, List<AbstractSlot>> resourceMap, int numTasks, int maxTaskOneNode) throws Exception {
		// slotId to slot mapping
		Map<String, AbstractSlot> res = new HashMap<>(numTasks);
		int numNodes = resourceMap.size();
		// todo, please ensure numTask could be divided by numNodes for experiment
		if (numTasks % numNodes != 0) {
			throw new Exception("please ensure numTask could be divided by numNodes for experiment");
		}
		System.out.println("++++++ number of tasks on each nodes: " + maxTaskOneNode);

		HashMap<String, Integer> loadMap = new HashMap<>();
		HashMap<String, Integer> pendingStots = new HashMap<>();
		HashMap<String, Integer> releasingStots = new HashMap<>();
		// compute the num of tasks in each node
		for (String nodeID : resourceMap.keySet()) {
			List<AbstractSlot> slotList = resourceMap.get(nodeID);
			for (AbstractSlot slot : slotList) {
				if (slot.getState() == AbstractSlot.State.ALLOCATED) {
					loadMap.put(nodeID, loadMap.getOrDefault(nodeID, 0) + 1);
					if (loadMap.getOrDefault(nodeID, 0) <= maxTaskOneNode) {
						res.put(slot.getId(), slot);
					}
				}
			}
		}

		// try to migrate uniformly
		for (String nodeID : resourceMap.keySet()) {
			// the node is overloaded, free future slots, and allocate a new slot in other nodes
			if (loadMap.getOrDefault(nodeID, 0) > maxTaskOneNode) {
				int nReleasingSlots = loadMap.getOrDefault(nodeID, 0) - maxTaskOneNode;
				releasingStots.put(nodeID, releasingStots.getOrDefault(nodeID, 0) + nReleasingSlots);
				for (int i = 0; i < nReleasingSlots; i++) {
					findUnusedSlot(maxTaskOneNode, loadMap, pendingStots, nodeID, resourceMap);
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

	/* ===================== changeOfLogic utility method ========================== */
	protected <T> T updateConstructorParameter(Class<T> funcClass, @Nullable Class<?>[] argsClass, Object... args) {
		try {
			if (argsClass == null){
				argsClass = Arrays.stream(args).map(o -> args.getClass()).toArray(Class[]::new);
			}
			return funcClass.getConstructor(argsClass).newInstance(args);
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			e.printStackTrace();
		}
		return null;
	}

	protected <T> T updateToNewClass(String funcClassName, @Nullable Class<?>[] argsClass, Object... args) {
		ClassLoader userLoader = getReconfigurationExecutor().getTrisk().getFunctionClassLoader();
		try {
			Class<T> funcClass = (Class<T>) userLoader.loadClass(funcClassName);
			return updateConstructorParameter(funcClass, argsClass, args);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	protected Object defineNewClass(String funcClassName, String funcClassCode,
								   @Nullable Class<?>[] argsClass, Object... args) {
		Class<?> funcClass = reconfigurationExecutor.registerFunctionClass(funcClassName, funcClassCode);
		return updateConstructorParameter(funcClass, argsClass, args);
	}

	private class ControlActionRunner extends Thread{
		@Override
		public void run() {
			try {
				defineControlAction();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
