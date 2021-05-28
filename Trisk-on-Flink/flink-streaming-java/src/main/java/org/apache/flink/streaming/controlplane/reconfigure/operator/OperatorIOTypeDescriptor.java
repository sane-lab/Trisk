package org.apache.flink.streaming.controlplane.reconfigure.operator;

import java.io.Serializable;

/**
 * contains the information about input, output, Stream Task type that could
 * help to check if one operator substitution is valid
 */
public class OperatorIOTypeDescriptor implements Serializable {
	Class inputType;
	Class outputType;

	OperatorIOTypeDescriptor(int operatorID){

	}

}
