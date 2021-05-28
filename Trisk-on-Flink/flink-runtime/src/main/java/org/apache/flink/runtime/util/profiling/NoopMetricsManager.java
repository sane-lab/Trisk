package org.apache.flink.runtime.util.profiling;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.io.Serializable;

public class NoopMetricsManager implements Serializable, MetricsManager {

	public NoopMetricsManager(String taskDescription, JobVertexID jobVertexId, Configuration jobConfiguration, int idInModel, int maximumKeygroups) {}

	@Override
	public void updateTaskId(String taskDescription, Integer idInModel) {

	}

	@Override
	public void newInputBuffer(long timestamp) {

	}

	@Override
	public String getJobVertexId() {
		return null;
	}

	@Override
	public void addSerialization(long serializationDuration) {

	}

	@Override
	public void addDeserialization(long deserializationDuration) {

	}

	@Override
	public void incRecordsOut() {

	}

	@Override
	public void incRecordsOutKeyGroup(int targetKeyGroup) {

	}

	@Override
	public void incRecordIn(int keyGroup) {

	}

	@Override
	public void addWaitingForWriteBufferDuration(long duration) {

	}

	@Override
	public void inputBufferConsumed(long timestamp, long deserializationDuration, long processing, long numRecords, long endToEndLatency) {

	}

	@Override
	public void groundTruth(int keyGroup, long arrivalTs, long completionTs) {

	}

	@Override
	public void groundTruth(long arrivalTs, long latency) {

	}

	@Override
	public void outputBufferFull(long timestamp) {

	}

	@Override
	public void updateMetrics() {

	}
}
