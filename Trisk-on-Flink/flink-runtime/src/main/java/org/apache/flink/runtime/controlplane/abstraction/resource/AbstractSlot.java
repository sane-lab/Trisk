package org.apache.flink.runtime.controlplane.abstraction.resource;

public interface AbstractSlot {
	State getState();

	Resource getResource();

	String getLocation();

	String getId();

	boolean isMatchingRequirement(Resource requirement);

	AbstractSlot copy();

	enum State {
		FREE,
		ALLOCATED,
		INUSE,
	}

}

