package org.apache.flink.runtime.rescale.controller;

import java.util.List;
import java.util.Map;

public interface OperatorControllerListener {

	void setup(Map<String, List<String>> executorMapping);

	void remap(Map<String, List<String>> executorMapping);

	void scale(int parallelism, Map<String, List<String>> executorMapping);
}
