package org.apache.flink.runtime.controlplane.abstraction;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class NodeDescriptor {
//		// host network address
//		InetAddress address;
//		// host number of cpus
//		int numCpus;
//		/* host memory in bytes */
//		int memory;

	// address id
	InetAddress nodeAddress;
	// number of slots
	int numOfSlots;

	// taskId = <Operator, taskIdx>
	List<TaskDescriptor> deployedTasks;

	public NodeDescriptor(InetAddress nodeAddress, int numOfSlots) {
//			this.address = address;
//			this.numCpus = numCpus;
//			this.memory = memory;
		this.nodeAddress = nodeAddress;
		this.numOfSlots = numOfSlots;
		deployedTasks = new ArrayList<>();
	}

	public NodeDescriptor copy() {
//			this.address = address;
//			this.numCpus = numCpus;
//			this.memory = memory;
		return new NodeDescriptor(this.nodeAddress, this.numOfSlots);
	}

	void addContainedTask(TaskDescriptor task) {
		deployedTasks.add(task);
	}
}
