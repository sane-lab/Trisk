package org.apache.flink.runtime.controlplane.abstraction;

public class TaskResourceDescriptor {
	// task own threads
	public String resourceSlot;
	NodeDescriptor location;
	// TODO: add the config here.

	public TaskResourceDescriptor(String resourceSlot, NodeDescriptor location) {
		this.resourceSlot = resourceSlot;
		this.location = location;
		// taskId = <Operator, taskIdx>
		location.addContainedTask(this);
	}

	public TaskResourceDescriptor copy(NodeDescriptor nodeCopy) {
		return new TaskResourceDescriptor(resourceSlot, nodeCopy);
	}
}
