package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.runtime.state.KeyGroupRange;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RemappingAssignment implements AbstractCoordinator.Diff {

	private final Map<Integer, List<Integer>> keymapping = new HashMap<>();
	private final Map<Integer, List<Integer>> oldKeymapping = new HashMap<>();
	private final List<KeyGroupRange> alignedKeyGroupRanges;

	public RemappingAssignment(Map<Integer, List<Integer>> newMapping, Map<Integer, List<Integer>> oldMapping) {
		for (int i = 0; i < newMapping.size(); i++) {
			keymapping.put(i, newMapping.get(i));
		}
		for (int i = 0; i < oldMapping.size(); i++) {
			oldKeymapping.put(i, oldMapping.get(i));
		}
		alignedKeyGroupRanges = generateAlignedKeyGroupRanges(newMapping);
	}

	public RemappingAssignment(Map<Integer, List<Integer>> mapping) {
		for (int i = 0; i < mapping.size(); i++) {
			keymapping.put(i, mapping.get(i));
		}
		alignedKeyGroupRanges = generateAlignedKeyGroupRanges(mapping);
	}

	private List<KeyGroupRange> generateAlignedKeyGroupRanges(Map<Integer, List<Integer>> partitionAssignment) {
		int keyGroupStart = 0;
		List<KeyGroupRange> alignedKeyGroupRanges = new ArrayList<>();
		for (List<Integer> list : partitionAssignment.values()) {
			int rangeSize = list.size();

			KeyGroupRange keyGroupRange = rangeSize == 0 ?
				KeyGroupRange.EMPTY_KEY_GROUP_RANGE :
				new KeyGroupRange(
					keyGroupStart,
					keyGroupStart + rangeSize - 1,
					list);

			alignedKeyGroupRanges.add(keyGroupRange);
			keyGroupStart += rangeSize;
		}
		return alignedKeyGroupRanges;
	}

	public boolean isTaskModified(int taskIndex){
		int minLen = Math.min(oldKeymapping.size(), keymapping.size());
		if(taskIndex >= minLen){
			return true;
		}
		return AbstractCoordinator.compareIntList(oldKeymapping.get(taskIndex), keymapping.get(taskIndex));
	}

	public boolean isScaleOut(){
		return keymapping.size() > oldKeymapping.size();
	}

	public Map<Integer, List<Integer>> getPartitionAssignment() {
		return keymapping;
	}

	public KeyGroupRange getAlignedKeyGroupRange(int subTaskIndex) {
		return alignedKeyGroupRanges.get(subTaskIndex);
	}
}
