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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * The MetricsManager is responsible for logging activity profiling information (except for messages).
 * It gathers start and end events for deserialization, processing, serialization, blocking on read and write buffers
 * and records activity durations. There is one MetricsManager instance per Task (operator instance).
 * The MetricsManager aggregates metrics in a {@link ProcessingStatus} object and outputs processing and output rates
 * periodically to a designated rates file.
 */
public class DefaultMetricsManager implements Serializable, MetricsManager {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricsManager.class);

	private String taskId; // Flink's task description
	private String workerName; // The task description string logged in the rates file
	private int instanceId; // The operator instance id
	private int numInstances; // The total number of instances for this operator
	private final JobVertexID jobVertexId; // To interact with StreamSwitch

	private long recordsIn = 0;	// Total number of records ingested since the last flush
	private long recordsOut = 0;	// Total number of records produced since the last flush
	private long usefulTime = 0;	// Total period of useful time since last flush
	private long waitingTime = 0;	// Total waiting time for input/output buffers since last flush
	private long latency = 0;	// Total end to end latency

	private long totalRecordsIn = 0;	// Total number of records ingested since the last flush
	private long totalRecordsOut = 0;	// Total number of records produced since the last flush

	private long currentWindowStart;

	private final ProcessingStatus status;

	private final long windowSize;	// The aggregation interval
//	private final String ratesPath;	// The file path where to output aggregated rates

	private long epoch = 0;	// The current aggregation interval. The MetricsManager outputs one rates file per epoch.

	private final String TOPIC;
	private final String STATUS_TOPIC = "flink_keygroups_status";	// status topic, used to do recovery.
	private final String servers;
	private int nRecords;
	private KafkaProducer<String, String> producer;
//	private KafkaProducer<String, String> statusProducer;

	private final int numKeygroups;

	private HashMap<Integer, Long> kgLatencyMap = new HashMap<>(); // keygroup -> avgLatency
	private HashMap<Integer, Integer> kgNRecordsMap = new HashMap<>(); // keygroup -> nRecords
	private long lastTimeSlot = 0l;


	/**
	 * @param taskDescription the String describing the owner operator instance
	 * @param jobConfiguration this job's configuration
	 */
	public DefaultMetricsManager(String taskDescription, JobVertexID jobVertexId, Configuration jobConfiguration, int idInModel, int maximumKeygroups) {
		numKeygroups = maximumKeygroups;

		taskId = taskDescription;
		String workerId = taskId.replace("Timestamps/Watermarks", "Timestamps-Watermarks");
		workerName = workerId.substring(0, workerId.indexOf("(")-1);
		instanceId = Integer.parseInt(workerId.substring(workerId.lastIndexOf("(")+1, workerId.lastIndexOf("/"))) - 1; // need to consistent with partitionAssignment
		instanceId = idInModel;
//		System.out.println("----updated task with instance id is: " + workerName + "-" + instanceId);
		System.out.println("start execution: " + workerName + "-" + instanceId + " time: " + System.currentTimeMillis());
		numInstances = Integer.parseInt(workerId.substring(workerId.lastIndexOf("/")+1, workerId.lastIndexOf(")")));
		status = new ProcessingStatus();

		windowSize = jobConfiguration.getLong("policy.windowSize",  1_000_000_000L);
		TOPIC = jobConfiguration.getString("policy.metrics.topic", "flink_metrics");
		servers = jobConfiguration.getString("policy.metrics.servers", "localhost:9092");
		nRecords = jobConfiguration.getInteger("policy.metrics.nrecords", 15);

		currentWindowStart = status.getProcessingStart();

		this.jobVertexId = jobVertexId;

		initKakfaProducer();
	}

	public void updateTaskId(String taskDescription, Integer idInModel) {
//		synchronized (status) {
		LOG.info("###### Starting update task metricsmanager from " + workerName + "-" + instanceId + " to " + workerName + "-" + idInModel);
		// not only need to update task id, but also the states.
		if (idInModel == Integer.MAX_VALUE/2) {
			System.out.println("end execution: " + workerName + "-" + instanceId + " time: " + System.currentTimeMillis());
		} else if (instanceId != idInModel && instanceId == Integer.MAX_VALUE/2) {
			System.out.println("start execution: " + workerName + "-" + idInModel + " time: " + System.currentTimeMillis());
		}

		instanceId = idInModel; // need to consistent with partitionAssignment

		taskId = taskDescription;
		String workerId = taskId.replace("Timestamps/Watermarks", "Timestamps-Watermarks");
		numInstances = Integer.parseInt(workerId.substring(workerId.lastIndexOf("/") + 1, workerId.lastIndexOf(")")));

		status.reset();

		totalRecordsIn = 0;	// Total number of records ingested since the last flush
		totalRecordsOut = 0;	// Total number of records produced since the last flush

		// clear counters
		recordsIn = 0;
		recordsOut = 0;
		usefulTime = 0;
		currentWindowStart = 0;
		latency = 0;
		epoch = 0;

		LOG.info("###### End update task metricsmanager");
//		}
	}

	@Override
	public String getJobVertexId() {
		return workerName + "-" + instanceId;
	}

	private void initKakfaProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", servers);
		props.put("client.id", workerName+instanceId);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);
	}

	/**
	 * Once the current input buffer has been consumed, calculate and log useful and waiting durations
	 * for this buffer.
	 * @param timestamp the end buffer timestamp
	 * @param processing total duration of processing for this buffer
	 * @param numRecords total number of records processed
	 */
	@Override
	public void inputBufferConsumed(long timestamp, long deserializationDuration, long processing, long numRecords, long endToEndLatency) {
		synchronized (status) {
			if (currentWindowStart == 0) {
				currentWindowStart = timestamp;
			}

			status.setProcessingEnd(timestamp);

			// aggregate the metrics
			recordsIn += numRecords;
			recordsOut += status.getNumRecordsOut();
			usefulTime += processing + deserializationDuration;
	//			usefulTime += processing + status.getSerializationDuration()
	//				- status.getWaitingForWriteBufferDuration();

			latency += endToEndLatency;

			// clear status counters
			status.clearCounters();

			// if window size is reached => output
			if (timestamp - currentWindowStart > windowSize) {
				// compute rates
				long duration = timestamp - currentWindowStart;
				double trueProcessingRate = (recordsIn / (usefulTime / 1000.0)) * 1000;
				double trueOutputRate = (recordsOut / (usefulTime / 1000.0)) * 1000;
				double observedProcessingRate = (recordsIn / (duration / 1000.0)) * 1000;
				double observedOutputRate = (recordsOut / (duration / 1000.0)) * 1000;
				float endToEndLantecy = (float) latency/recordsIn;

				double utilization = (double) usefulTime / duration;

				// for network calculus
//				totalRecordsIn += recordsIn;
//				totalRecordsOut += recordsOut;

				String keyGroupOutput = new String();
				if (!status.outputKeyGroup.isEmpty()) {
					for (Map.Entry<Integer, Long> entry : status.outputKeyGroupState.entrySet()) {
						int partitionId = entry.getKey();
						keyGroupOutput += partitionId + ":"
							+ status.outputKeyGroup.getOrDefault(partitionId, 0l) + "&";
						totalRecordsOut += status.outputKeyGroup.getOrDefault(partitionId, 0l);
					}
					keyGroupOutput = keyGroupOutput.substring(0, keyGroupOutput.length() - 1);
				} else {
					keyGroupOutput = "0";
				}

				String keyGroupinput = new String();
				if (!status.inputKeyGroup.isEmpty()) {
					for (Map.Entry<Integer, Long> entry : status.inputKeyGroupState.entrySet()) {
						int partitionId = entry.getKey();
						keyGroupinput += partitionId + ":"
							+ status.inputKeyGroup.getOrDefault(partitionId, 0l) + "&";

						totalRecordsIn += status.inputKeyGroup.getOrDefault(partitionId, 0l);
					}
					keyGroupinput = keyGroupinput.substring(0, keyGroupinput.length() - 1);
				} else {
					keyGroupinput = "0";
				}

				// log the rates: one file per epoch
				String ratesLine = jobVertexId + ","
					+ workerName + "-" + instanceId + ","
					+ numInstances  + ","
					+ trueProcessingRate + ","
					+ trueOutputRate + ","
					+ observedProcessingRate + ","
					+ observedOutputRate + ","
					+ endToEndLantecy + ","
					+ totalRecordsIn + ","
					+ totalRecordsOut + ","
					+ utilization + ","
					+ keyGroupOutput + ","
					+ keyGroupinput + ","
					+ System.currentTimeMillis();

	//				String ratesLine = jobVertexId + ","
	//					+ workerName + "-" + instanceId + ","
	//					+ " trueProcessingRate: " + trueProcessingRate + ","
	//					+ " observedProcessingRate: " + observedProcessingRate + ","
	//					+ " endToEndLantecy: " + endToEndLantecy + ","
	//					+ " utilization: " + utilization;

	//				System.out.println("workername: " + getJobVertexId() + " epoch: " + epoch + " keygroups: " + status.inputKeyGroup.keySet());

				ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, ratesLine);
				producer.send(newRecord);

				// clear counters
				recordsIn = 0;
				recordsOut = 0;
				usefulTime = 0;
				currentWindowStart = 0;
				latency = 0;
				epoch++;
				// clear keygroups every time, because we are using delta here
				status.clearKeygroups();
			}
		}
	}

	@Override
	public void groundTruth(int keyGroup, long arrivalTs, long completionTs) {
		if (completionTs - lastTimeSlot >= 1000) {
			// print out to stdout, and clear the state
			Iterator it = kgLatencyMap.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry kv = (Map.Entry)it.next();
				int curKg = (int) kv.getKey();
				long sumLatency = (long) kv.getValue();
				int nRecords = kgNRecordsMap.get(curKg);
				float avgLatency = (float) sumLatency / nRecords;
//				System.out.println("timeslot: " + lastTimeSlot + " keygroup: "
//					+ curKg + " records: " + nRecords + " avglatency: " + avgLatency);
				System.out.println(String.format(jobVertexId.toString() + " GroundTruth: %d %d %d %f", lastTimeSlot, curKg, nRecords, avgLatency));
			}
			kgLatencyMap.clear();
			kgNRecordsMap.clear();
			lastTimeSlot = completionTs / 1000 * 1000;
		}
		kgNRecordsMap.put(keyGroup,
			kgNRecordsMap.getOrDefault(keyGroup, 0)+1);
		kgLatencyMap.put(keyGroup,
			kgLatencyMap.getOrDefault(keyGroup, 0L)+(completionTs - arrivalTs));
	}

	@Override
	public void groundTruth(long arrivalTs, long latency) {

	}


	/**
	 * A new input buffer has been retrieved with the given timestamp.
	 */
	@Override
	public void newInputBuffer(long timestamp) {
		status.setProcessingStart(timestamp);
		// the time between the end of the previous buffer processing and timestamp is "waiting for input" time
		status.setWaitingForReadBufferDuration(timestamp - status.getProcessingEnd());
	}

	@Override
	public void addSerialization(long serializationDuration) {
		status.addSerialization(serializationDuration);
	}

	@Override
	public void addDeserialization(long deserializationDuration) {

	}

	@Override
	public void incRecordsOut() {
		status.incRecordsOut();
	}

	@Override
	public void incRecordsOutKeyGroup(int targetKeyGroup) {
		status.incRecordsOutChannel(targetKeyGroup);

	}

	@Override
	public void incRecordIn(int keyGroup) {
		status.incRecordsIn(keyGroup);
	}

	@Override
	public void addWaitingForWriteBufferDuration(long duration) {
		status.addWaitingForWriteBuffer(duration);

	}

	/**
	 * The source consumes no input, thus it must log metrics whenever it writes an output buffer.
	 * @param timestamp the timestamp when the current output buffer got full.
	 */
	@Override
	public void outputBufferFull(long timestamp) {
		if (taskId.contains("Source")) {

			synchronized (status) {

				if (currentWindowStart == 0) {
					currentWindowStart = timestamp;
				}

				setOutBufferStart(timestamp);

				// aggregate the metrics
				recordsOut += status.getNumRecordsOut();
				if (status.getWaitingForWriteBufferDuration() > 0) {
					waitingTime += status.getWaitingForWriteBufferDuration();
				}

				// clear status counters
				status.clearCounters();

				// if window size is reached => output
				if (timestamp - currentWindowStart > windowSize) {

					// compute rates
					long duration = timestamp - currentWindowStart;
					usefulTime = duration - waitingTime;
					double trueOutputRate = (recordsOut / (usefulTime / 1000.0)) * 1000;
					double observedOutputRate = (recordsOut / (duration / 1000.0)) * 1000;
	//				totalRecordsOut += recordsOut;

					String keyGroupOutput = new String();
					if (!status.outputKeyGroup.isEmpty()) {
						for (Map.Entry<Integer, Long> entry : status.outputKeyGroupState.entrySet()) {
							int partitionId = entry.getKey();
							keyGroupOutput += partitionId + ":"
								+ status.outputKeyGroup.getOrDefault(partitionId, 0l) + "&";
							totalRecordsOut += status.outputKeyGroup.getOrDefault(partitionId, 0l);
						}
						keyGroupOutput = keyGroupOutput.substring(0, keyGroupOutput.length() - 1);
					} else {
						keyGroupOutput = "0";
					}

					// log the rates: one file per epoch
					String ratesLine = jobVertexId + ","
						+ workerName + "-" + instanceId + ","
						+ numInstances  + ","
						+ currentWindowStart + ","
						+ 0 + ","
	//						+ trueOutputRate + ","
						+ 0 + ","
						+ observedOutputRate + ","
						+ 0 + "," // end to end latency should be 0.
						+ 0 + ","
						+ totalRecordsOut + ","
						+ 0 + ","
						+ keyGroupOutput + ","
						+ 0 + ","
						+ System.currentTimeMillis();
					List<String> rates = Arrays.asList(ratesLine);
					ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, ratesLine);
					producer.send(newRecord);

	//					Path ratesFile = Paths.get(ratesPath + workerName.trim() + "-" + instanceId + "-" + epoch + ".log").toAbsolutePath();
	//					try {
	//						Files.write(ratesFile, rates, Charset.forName("UTF-8"));
	//					} catch (IOException e) {
	//						System.err.println("Error while writing rates file for epoch " + epoch
	//							+ " on task " + taskId + ".");
	//						e.printStackTrace();
	//					}

					// clear counters
					recordsOut = 0;
					usefulTime = 0;
					waitingTime = 0;
					currentWindowStart = 0;
					epoch++;
					// clear keygroups for delta
					status.clearKeygroups();
				}
			}
		}
	}

	private void setOutBufferStart(long start) {
		status.setOutBufferStart(start);
	}


	@Override
	public void updateMetrics() {
		if (instanceId == Integer.MAX_VALUE/2) {
			return;
		}

		synchronized (status) {

			// compute rates
			// for network calculus
			totalRecordsIn += recordsIn;
			totalRecordsOut += recordsOut;


			// compute rates
			long duration = System.nanoTime() - currentWindowStart;
			double trueProcessingRate = (recordsIn / (usefulTime / 1000.0)) * 1000;
			double trueOutputRate = (recordsOut / (usefulTime / 1000.0)) * 1000;
			double observedProcessingRate = (recordsIn / (duration / 1000.0)) * 1000;
			double observedOutputRate = (recordsOut / (duration / 1000.0)) * 1000;
			float endToEndLantecy = (float) latency/recordsIn;

			double utilization = (double) usefulTime / duration;

			String keyGroupOutput = new String();
			if (!status.outputKeyGroup.isEmpty()) {
				for (Map.Entry<Integer, Long> entry : status.outputKeyGroupState.entrySet()) {
					int partitionId = entry.getKey();
					keyGroupOutput += partitionId + ":"
						+ status.outputKeyGroup.getOrDefault(partitionId, 0l) + "&";
					totalRecordsOut += status.outputKeyGroup.getOrDefault(partitionId, 0l);
				}
				keyGroupOutput = keyGroupOutput.substring(0, keyGroupOutput.length() - 1);
			} else {
				keyGroupOutput = "0";
			}

			String keyGroupinput = new String();
			if (!status.inputKeyGroup.isEmpty()) {
				for (Map.Entry<Integer, Long> entry : status.inputKeyGroupState.entrySet()) {
					int partitionId = entry.getKey();
					keyGroupinput += partitionId + ":"
						+ status.inputKeyGroup.getOrDefault(partitionId, 0l) + "&";

					totalRecordsIn += status.inputKeyGroup.getOrDefault(partitionId, 0l);
				}
				keyGroupinput = keyGroupinput.substring(0, keyGroupinput.length() - 1);
			} else {
				keyGroupinput = "0";
			}

			// log the rates: one file per epoch
			String ratesLine = jobVertexId + ","
				+ workerName + "-" + instanceId  + ","
				+ numInstances  + ","
				+ trueProcessingRate + ","
				+ 0 + ","
				+ 0 + ","
				+ 0 + ","
				+ 0 + ","
				+ totalRecordsIn + ","
				+ totalRecordsOut + ","
				+ 0.1 + ","
				+ keyGroupOutput + ","
				+ keyGroupinput + ","
				+ System.currentTimeMillis();

//			if (taskId.contains("MatchMaker")) {
//				LOG.info("++++++force update - worker: " + workerName + "-" + instanceId + " keygroups processed:" + keyGroupinput);
//			} else if (taskId.contains("Source")) {
//				LOG.info("++++++force update - worker: " + workerName + "-" + instanceId + " keygroups output:" + keyGroupOutput);
//			}
			// send to flink_metrics topic, for metrics retriever
			ProducerRecord<String, String> metricsRecord = new ProducerRecord<>(TOPIC, ratesLine);
			producer.send(metricsRecord);

//			if (taskId.contains("MatchMaker") || taskId.contains("Source")) {
//				LOG.info("++++++force update - worker: " + workerName + "-" + instanceId + " Completed!");
//			}
			// clear counters
			recordsIn = 0;
			recordsOut = 0;
			usefulTime = 0;
			currentWindowStart = 0;
			latency = 0;
			epoch++;
			// clear keygroups for delta
			status.clearKeygroups();
		}
	}
}
