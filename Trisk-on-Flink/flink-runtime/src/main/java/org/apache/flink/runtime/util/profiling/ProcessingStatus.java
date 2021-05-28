/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util.profiling;

import java.io.Serializable;
import java.util.HashMap;

/**
 * This class maintains information regarding the processing progress of a worker per input buffer.
 * It updates activity durations so that the {@link MetricsManager} can log accurate profiling data.
 */
public class ProcessingStatus implements Serializable {

	// the timestamp of an the event that we got a new buffer to process
	private long processingStart;

	private long processingEnd;

	// the total time spent on serialization for this buffer so far
	private long serializationDuration;
	private long deserializationDuration;

	private long outputRecords;

	// the total time spent waiting to acquire a write buffer (to be subtracted from processing)
	private long waitingForWriteBufferDuration;

	private long waitingForReadBufferDuration;

	private long outBufferStart;

	public HashMap<Integer, Long> outputKeyGroup = new HashMap<>();
	public HashMap<Integer, Long> inputKeyGroup = new HashMap<>();

	public HashMap<Integer, Long> inputKeyGroupState = new HashMap<>();
	public HashMap<Integer, Long> outputKeyGroupState = new HashMap<>();

	ProcessingStatus() {
		processingStart = System.nanoTime();
		processingEnd = System.nanoTime();
		serializationDuration = 0;
		deserializationDuration = 0;
		waitingForReadBufferDuration = 0;
		waitingForWriteBufferDuration = 0;
		outputRecords = 0;
		outBufferStart = System.nanoTime();

		outputKeyGroup.clear();
		inputKeyGroup.clear();

	}

	void reset() {
		processingStart = System.nanoTime();
		processingEnd = System.nanoTime();
		serializationDuration = 0;
		deserializationDuration = 0;
		waitingForReadBufferDuration = 0;
		waitingForWriteBufferDuration = 0;
		outputRecords = 0;
		outBufferStart = System.nanoTime();

		outputKeyGroup.clear();
		inputKeyGroup.clear();

		inputKeyGroupState.clear();
		outputKeyGroupState.clear();
	}

	void clearKeygroups() {
		outputKeyGroup.clear();
		inputKeyGroup.clear();
	}

	long getProcessingEnd() {
		return processingEnd;
	}

	void setOutBufferStart(long start) {
		outBufferStart = start;
	}

	void setProcessingStart(long processingStart) {
		this.processingStart = processingStart;
	}

	long getProcessingStart() {
		return processingStart;
	}

	void setProcessingEnd(long processingEnd) {
		this.processingEnd = processingEnd;
	}

	long getSerializationDuration() {
		return serializationDuration;
	}

	long getDeserializationDuration() {
		return deserializationDuration;
	}

	void addSerialization(long duration) {
		serializationDuration += duration;
	}

	void addDeserialization(long duration) {
		deserializationDuration += duration;
	}

	long getWaitingForWriteBufferDuration() {
		return waitingForWriteBufferDuration;
	}

	void addWaitingForWriteBuffer(long duration) {
		waitingForWriteBufferDuration += duration;
	}

	void setWaitingForReadBufferDuration(long duration) {
		this.waitingForReadBufferDuration = duration;
	}

	public long getNumRecordsOut() {
		return outputRecords;
	}

	void clearCounters() {
		serializationDuration = 0;
		deserializationDuration = 0;
		waitingForWriteBufferDuration = 0;
		waitingForReadBufferDuration = 0;
		outputRecords = 0;
	}

	void incRecordsOut() {
		outputRecords++;
	}

	void incRecordsOutChannel(int targetKeyGroup) {
		outputKeyGroupState.putIfAbsent(targetKeyGroup, 0l);
		outputKeyGroup.put(targetKeyGroup, outputKeyGroup.getOrDefault(targetKeyGroup, 0l)+1);
	}

	void incRecordsIn(int keyGroup) {
		inputKeyGroupState.putIfAbsent(keyGroup, 0l);
		inputKeyGroup.put(keyGroup, inputKeyGroup.getOrDefault(keyGroup, 0l)+1);
	}
}
