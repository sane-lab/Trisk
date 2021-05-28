package org.apache.flink.runtime.controlplane.abstraction;

public class TaskDescriptor {
	// task own threads
	public String resourceSlot;
	NodeDescriptor location;
	// TODO: add the config here.

	public TaskDescriptor(String resourceSlot, NodeDescriptor location) {
		this.resourceSlot = resourceSlot;
		this.location = location;
		// taskId = <Operator, taskIdx>
		location.addContainedTask(this);
	}

	public TaskDescriptor copy(NodeDescriptor nodeCopy) {
		return new TaskDescriptor(resourceSlot, nodeCopy);
	}
}
