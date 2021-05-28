package org.apache.flink.runtime.controlplane.abstraction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

@Internal
public class OperatorDescriptorVisitor {

	private OperatorDescriptor operatorDescriptor;

	private final static OperatorDescriptorVisitor INSTANCE = new OperatorDescriptorVisitor();

	public static OperatorDescriptorVisitor attachOperator(OperatorDescriptor operatorDescriptor) {
		INSTANCE.operatorDescriptor = operatorDescriptor;
		return INSTANCE;
	}

	public void addChildren(List<Tuple2<Integer, Integer>> childEdges, Map<Integer, OperatorDescriptor> allOperatorsById) {
		operatorDescriptor.addChildren(childEdges, allOperatorsById);
	}

	public void addParent(List<Tuple2<Integer, Integer>> parentEdges, Map<Integer, OperatorDescriptor> allOperatorsById) {
		operatorDescriptor.addParent(parentEdges, allOperatorsById);
	}

	public void setKeyStateAllocation(Map<Integer, List<Integer>> keyStateAllocation) {
		operatorDescriptor.setKeyStateAllocation(keyStateAllocation);
	}

	public void setKeyMapping(Map<Integer, Map<Integer, List<Integer>>> keyMapping) {
		operatorDescriptor.setKeyMapping(keyMapping);
	}

	public void setAttributeField(Object object, List<Field> fieldList) throws IllegalAccessException {
		operatorDescriptor.setAttributeField(object, fieldList);
	}

	public OperatorDescriptor.ExecutionLogic getApplicationLogic() {
		return operatorDescriptor.getApplicationLogic();
	}
}
