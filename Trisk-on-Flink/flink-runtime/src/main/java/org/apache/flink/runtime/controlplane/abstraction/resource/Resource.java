package org.apache.flink.runtime.controlplane.abstraction.resource;

import org.apache.flink.annotation.VisibleForTesting;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class Resource {
	/**
	 * A Resource that indicates infinite resource that matches any resource requirement, for testability purpose only.
	 */
	@VisibleForTesting
	public static final Resource ANY = newBuilder()
		.setCpuCores(Double.MAX_VALUE)
		.setTaskHeapMemory(Long.MAX_VALUE)
		.setTaskOffHeapMemory(Long.MAX_VALUE)
		.setManagedMemory(Long.MAX_VALUE)
		.setNetworkMemory(Long.MAX_VALUE)
		.build();

	/** A Resource describing zero resources. */
	public static final Resource ZERO = newBuilder().build();

	private final double cpuCores;

	/** How much task heap memory is needed. */
	private final long taskHeapMemory;

	/** How much task off-heap memory is needed. */
	private final long taskOffHeapMemory;

	/** How much managed memory is needed. */
	private final long managedMemory;

	/** How much network memory is needed. */
	private final long networkMemory;


	private Resource(
		final double cpuCores,
		final long taskHeapMemory,
		final long taskOffHeapMemory,
		final long managedMemory,
		final long networkMemory) {

		this.cpuCores = cpuCores;
		this.taskHeapMemory = taskHeapMemory;
		this.taskOffHeapMemory = taskOffHeapMemory;
		this.managedMemory = managedMemory;
		this.networkMemory = networkMemory;
	}

	public boolean isMatching(final Resource required) {
		checkNotNull(required, "Cannot check matching with null resources");

		if (this.equals(ANY)) {
			return true;
		}

		if (this.equals(required)) {
			return true;
		}

		if (required.equals(ZERO)) {
			return true;
		}

		if (this.equals(ZERO)) {
			return false;
		}

		if (cpuCores >= required.cpuCores &&
			taskHeapMemory >= required.taskHeapMemory &&
			taskOffHeapMemory >= required.taskOffHeapMemory &&
			managedMemory >= required.managedMemory &&
			networkMemory >= required.networkMemory) {
			return true;
		}
		return false;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		int result = Objects.hashCode(cpuCores);
		result = 31 * result + Objects.hashCode(taskHeapMemory);
		result = 31 * result + Objects.hashCode(taskOffHeapMemory);
		result = 31 * result + Objects.hashCode(managedMemory);
		result = 31 * result + Objects.hashCode(networkMemory);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == Resource.class) {
			Resource that = (Resource) obj;
			return Objects.equals(this.cpuCores, that.cpuCores) &&
				Objects.equals(taskHeapMemory, that.taskHeapMemory) &&
				Objects.equals(taskOffHeapMemory, that.taskOffHeapMemory) &&
				Objects.equals(managedMemory, that.managedMemory) &&
				Objects.equals(networkMemory, that.networkMemory);
		}
		return false;
	}

	@Override
	public String toString() {
		if (this.equals(ZERO)) {
			return "Resource{ZERO}";
		}

		if (this.equals(ANY)) {
			return "Resource{ANY}";
		}

		return "Resource{" + getResourceString() + '}';
	}

	private String getResourceString() {
		String resourceStr = "cpuCores=" + cpuCores;
		resourceStr = addMemorySizeString(resourceStr, "taskHeapMemory", taskHeapMemory);
		resourceStr = addMemorySizeString(resourceStr, "taskOffHeapMemory", taskOffHeapMemory);
		resourceStr = addMemorySizeString(resourceStr, "managedMemory", managedMemory);
		resourceStr = addMemorySizeString(resourceStr, "networkMemory", networkMemory);
		return resourceStr;
	}

	private static String addMemorySizeString(String resourceStr, String name, long size) {
		String comma = resourceStr.isEmpty() ? "" : ", ";
		String memorySizeStr = comma + name + '=' + size;
		return resourceStr + memorySizeStr;
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * Builder for the {@link Resource}.
	 */
	public static class Builder {

		private double cpuCores = 0.0;
		private long taskHeapMemory = 0L;
		private long taskOffHeapMemory = 0L;
		private long managedMemory = 0L;
		private long networkMemory = 0L;

		private Builder() {
		}

		public Builder setCpuCores(double cpuCores) {
			this.cpuCores = cpuCores;
			return this;
		}

		public Builder setTaskHeapMemory(long taskHeapMemory) {
			this.taskHeapMemory = taskHeapMemory;
			return this;
		}

		public Builder setTaskHeapMemoryMB(int taskHeapMemoryMB) {
			this.taskHeapMemory = ((long) taskHeapMemoryMB) << 20;
			return this;
		}

		public Builder setTaskOffHeapMemory(long taskOffHeapMemory) {
			this.taskOffHeapMemory = taskOffHeapMemory;
			return this;
		}

		public Builder setTaskOffHeapMemoryMB(int taskOffHeapMemoryMB) {
			this.taskOffHeapMemory = ((long) taskOffHeapMemoryMB) << 20;
			return this;
		}

		public Builder setManagedMemory(long managedMemory) {
			this.managedMemory = managedMemory;
			return this;
		}

		public Builder setManagedMemoryMB(int managedMemoryMB) {
			this.managedMemory = ((long) managedMemoryMB) << 20;
			return this;
		}

		public Builder setNetworkMemory(long networkMemory) {
			this.networkMemory = networkMemory;
			return this;
		}

		public Builder setNetworkMemoryMB(int networkMemoryMB) {
			this.networkMemory = ((long) networkMemoryMB) << 20;
			return this;
		}

		public Resource build() {
			return new Resource(
				cpuCores,
				taskHeapMemory,
				taskOffHeapMemory,
				managedMemory,
				networkMemory);
		}
	}
}
