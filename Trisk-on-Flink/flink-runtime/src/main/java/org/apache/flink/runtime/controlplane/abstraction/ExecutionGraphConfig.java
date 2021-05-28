package org.apache.flink.runtime.controlplane.abstraction;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Deployment state should be get in real time since it is changed due to streaming system fail/restart strategy
 */
public interface ExecutionGraphConfig {
	/**
	 * Get all hosts of current job
	 *
	 * @return
	 */
	Node[] getResourceDistribution();

	/**
	 * Get one of the parallel task of one operator.
	 *
	 * @param operatorID the operator id of this operator
	 * @param taskId     represent which task instance of this operator
	 * @return
	 */
	Task getTask(Integer operatorID, int taskId);

	class Task {
		// task own threads
		int allocatedSlot;
		Node location;

		public Task(int allocatedSlot, Node location) {
			this.allocatedSlot = allocatedSlot;
			this.location = location;
			location.addContainedTask(this);
		}
	}

	class Node {
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

		List<Task> deployedTasks;

		public Node(InetAddress nodeAddress, int numOfSlots) {
//			this.address = address;
//			this.numCpus = numCpus;
//			this.memory = memory;
			this.nodeAddress = nodeAddress;
			this.numOfSlots = numOfSlots;
			deployedTasks = new ArrayList<>();
		}

		void addContainedTask(Task task){
			deployedTasks.add(task);
		}
	}
}
