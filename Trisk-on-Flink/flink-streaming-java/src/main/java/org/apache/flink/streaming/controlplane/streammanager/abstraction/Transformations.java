package org.apache.flink.streaming.controlplane.streammanager.abstraction;

import java.util.List;
import java.util.Map;

public class Transformations {
	private Map<String, Map<Integer, List<Integer>>> transformations;
	// operator -> tasks
	private Map<Integer, List<Integer>> tasks;

	public Transformations() {

	}

	public void add(String operation, Map<Integer, List<Integer>> transformation) {
		transformations.put(operation, transformation);
		for (Integer operatorID : transformation.keySet()) {
			tasks.putIfAbsent(operatorID, transformation.get(operatorID));
		}
	}

	public Map<Integer, List<Integer>> getTasks() {
		return tasks;
	}

	public Map<String, Map<Integer, List<Integer>>> getTransformations() {
		return transformations;
	}
}
