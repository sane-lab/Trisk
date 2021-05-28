package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AssignedKeyGroupStreamPartitioner<T, K> extends StreamPartitioner<T> implements ConfigurableStreamPartitioner {
	private static final long serialVersionUID = 1L;

	private final KeySelector<T, K> keySelector;

	private int maxParallelism;

	// map of (keyGroupId, subTaskIndex)
	private Map<Integer, Integer> assignKeyToOperator;

	public AssignedKeyGroupStreamPartitioner(KeySelector<T, K> keySelector, int maxParallelism, Map<Integer, List<Integer>> partitionAssignment) {
		Preconditions.checkArgument(maxParallelism > 0, "Number of key-groups must be > 0!");
		this.keySelector = Preconditions.checkNotNull(keySelector);
		this.maxParallelism = maxParallelism;
		this.assignKeyToOperator = getAssignKeyToOperator(partitionAssignment);
	}

	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		K key;
		try {
			key = keySelector.getKey(record.getInstance().getValue());
		} catch (Exception e) {
			throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
		}

		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
		int selectedChannel = assignKeyToOperator.get(keyGroup);
		Preconditions.checkState(selectedChannel >= 0 && selectedChannel < numberOfChannels, "selected channel out of range , "
			+ metricsManager.getJobVertexId() + ": " + selectedChannel + " / " + numberOfChannels);

		record.getInstance().setKeyGroup(keyGroup);
		metricsManager.incRecordsOutKeyGroup(record.getInstance().getKeyGroup());

		return selectedChannel;
	}

	public Map<Integer, List<Integer>> getKeyMappingInfo(int parallelism){
		Map<Integer, List<Integer>> keyStateAllocation = new HashMap<>();
		for(int channelIndex=0; channelIndex < parallelism; channelIndex++) {
			keyStateAllocation.put(channelIndex, new ArrayList<>());
		}
		for(Integer key: assignKeyToOperator.keySet()){
			keyStateAllocation.get(assignKeyToOperator.get(key)).add(key);
		}
		return keyStateAllocation;
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "REHASHED";
	}

	@Override
	public void configure(int maxParallelism) {
		KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
		this.maxParallelism = maxParallelism;
	}

	public void updateNewPartitionAssignment(Map<Integer, List<Integer>> partitionAssignment) {
		this.assignKeyToOperator = getAssignKeyToOperator(partitionAssignment);
	}

	private static Map<Integer, Integer> getAssignKeyToOperator(Map<Integer, List<Integer>> partitionAssignment) {
		Map<Integer, Integer> assignKeyToOperator = new HashMap<>();
		for (Integer subTaskIndex : partitionAssignment.keySet()) {
			for (Integer keyGroup : partitionAssignment.get(subTaskIndex)) {
				if (assignKeyToOperator.putIfAbsent(keyGroup, subTaskIndex) != null) {
					throw new IllegalArgumentException("invalid partitionAssignment");
				}
			}
		}
		return assignKeyToOperator;
	}
}
