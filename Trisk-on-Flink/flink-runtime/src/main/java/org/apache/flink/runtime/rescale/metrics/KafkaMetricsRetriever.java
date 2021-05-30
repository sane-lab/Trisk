package org.apache.flink.runtime.rescale.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rescale.JobRescalePartitionAssignment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaMetricsRetriever implements StreamSwitchMetricsRetriever {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricsRetriever.class);
	private AtomicBoolean closed = new AtomicBoolean();
	private CountDownLatch shutdownLatch = new CountDownLatch(1);
	private String servers;

	Set<String> containerIds;

	private Configuration jobConfiguration;
	private String TOPIC;
	private KafkaConsumer<String, String> consumer;
	private JobGraph jobGraph;
	private JobVertexID vertexID;
	private List<JobVertexID> upstreamVertexIDs = new ArrayList<>();
	private int nRecords;

	private long startTs = System.currentTimeMillis();
	private int metrcsWarmUpTime;
	private int numPartitions;

	private HashMap<String, Long> workerTimeStamp = new HashMap<>(); // latest timestamp of each partition, timestamp smaller than value in this should be invalid
	private HashMap<String, Double> lastExecutorServiceRate = new HashMap<>();
//	private HashMap<String, Long> lastpartitionArrived = new HashMap<>();
	private double initialParallelism;

	private HashMap<String, Long> partitionArrivedState = new HashMap<>();
	private HashMap<String, Long> partitionProcessedState = new HashMap<>();

	private long recordsCnt = 0;
	private long lastoffset = 0;

	@Override
	public void init(JobGraph jobGraph, JobVertexID vertexID, Configuration jobConfiguration, int numPartitions, int parallelism) {
		this.jobGraph = jobGraph;
		this.vertexID = vertexID;

		this.jobConfiguration = jobConfiguration;
		TOPIC = jobConfiguration.getString("policy.metrics.topic", "flink_metrics");
		servers = jobConfiguration.getString("policy.metrics.servers", "localhost:9092");
		nRecords = jobConfiguration.getInteger("model.retrieve.nrecords", 15);
		metrcsWarmUpTime = jobConfiguration.getInteger("model.metrics.warmup", 100);

		JobVertex curVertex = jobGraph.findVertexByID(vertexID);
		for (JobEdge jobEdge : curVertex.getInputs()) {
			JobVertexID id = jobEdge.getSource().getProducer().getID();
			upstreamVertexIDs.add(id);
		}

		this.numPartitions = numPartitions;
		this.initialParallelism = parallelism;
		initConsumer();
	}

	public void initConsumer(){
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, vertexID.toString());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer(props);
		consumer.subscribe(Arrays.asList(TOPIC));

		LOG.info("consumer object: " + consumer.hashCode());

		ConsumerRecords<String, String> records = consumer.poll(100);
		Set<TopicPartition> assignedPartitions = consumer.assignment();
		for (TopicPartition partition : assignedPartitions) {
			consumer.seek(partition, 0);
			long endPosition = consumer.position(partition);
//			LOG.info("cur vertex: " + vertexID.toString() + " start offset: " + endPosition);
		}
	}

	@Override
	public Map<String, Object> retrieveMetrics() {
		// TODO: source operator should be skipped
		// retrieve metrics from Kafka, consume topic and get upstream and current operator metrics

		Map<String, Object> metrics = new HashMap<>();

		// used to store delta of metrics
		HashMap<String, Long> partitionArrivedDelta = new HashMap<>();
		HashMap<String, Long> partitionProcessedDelta = new HashMap<>();
		// used to output
		HashMap<String, Long> partitionArrived = new HashMap<>();
		HashMap<String, Long> partitionProcessed = new HashMap<>();
		HashMap<String, Boolean> partitionValid = new HashMap<>();

		HashMap<String, Double> executorUtilization = new HashMap<>();
		HashMap<String, Double> executorServiceRate = new HashMap<>();

		// to record the timestamp of current worker for update
		HashMap<String, Long> workerCurTimeStamp = new HashMap<>();


		HashMap<String, HashMap<String, Long>> upstreamArrived = new HashMap<>(); // store all executors, not just vertex id

		synchronized (consumer) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			Set<TopicPartition> assignedPartitions = consumer.assignment();
			for (TopicPartition partition : assignedPartitions) {
//				System.out.println(vertexID + ": " + consumer.position(partition));
				long endPosition = consumer.position(partition);
//				LOG.info("cur vertex: " + vertexID.toString()
//					+ " consumer: " + consumer.hashCode() + " cur offset: " + endPosition);
			}
			if (!records.isEmpty()) {
				// parse records, should construct metrics hashmap
				for (ConsumerRecord<String, String> record : records) {
					if (record.value().equals("")) {
						continue;
					}
					recordsCnt++;
					String[] ratesLine = record.value().split(",");
					JobVertexID jobVertexId = JobVertexID.fromHexString(ratesLine[0]);
					if (jobVertexId.equals(this.vertexID)) {
						retrievePartitionProcessed(ratesLine, partitionProcessedDelta, executorUtilization
							, executorServiceRate, workerTimeStamp, workerCurTimeStamp);
					}

					// find upstream numRecordOut
					if (upstreamVertexIDs.contains(jobVertexId)) {
						// get executor id
						String upstreamExecutorId = ratesLine[1];
						// put into the corresponding upstream queue, aggregate later
						HashMap<String, Long> curUpstreamArrived = upstreamArrived.getOrDefault(upstreamExecutorId, new HashMap<>());
						retrievePartitionArrived(ratesLine, curUpstreamArrived, workerTimeStamp, workerCurTimeStamp);
						upstreamArrived.put(upstreamExecutorId, curUpstreamArrived);
					}
				}
			}

//			LOG.info("cur vertex: " + vertexID.toString() + " records valid cnt: " + recordsCnt);
		}

		// aggregate partitionArrived
		for (Map.Entry entry : upstreamArrived.entrySet()) {
			for (Map.Entry subEntry : ((HashMap<String, Long>) entry.getValue()).entrySet()) {
				String keygroup = (String) subEntry.getKey();
				long keygroupArrived = (long) subEntry.getValue();
				partitionArrivedDelta.put(keygroup, partitionArrivedDelta.getOrDefault(keygroup, 0l) + keygroupArrived);
			}
		}

		if (System.currentTimeMillis() - startTs < metrcsWarmUpTime*1000
			&& partitionProcessedDelta.size() == 0 && partitionArrivedDelta.size() == 0) {
			// make all metrics be 0.0
			for (int i=0; i<numPartitions; i++) {
				String keyGroup = String.valueOf(i);
				partitionArrivedDelta.put(keyGroup, 0l);
				partitionProcessedDelta.put(keyGroup, 0l);
				partitionValid.put(keyGroup, true);
			}
			for (int i=0; i<initialParallelism; i++) {
				String executorId = i+"";
				executorUtilization.put(executorId, 0.01);
			}
		}

		for (int i=0; i<numPartitions; i++) {
			String partitionId = String.valueOf(i);
			// update arrived if arrival has delta
			if (partitionArrivedDelta.containsKey(partitionId)) {
				// sum delta and state to get total processed for metrics
				partitionArrivedState.put(partitionId,
					partitionArrivedState.getOrDefault(partitionId, 0l)+partitionArrivedDelta.get(partitionId));
			}
			// save the latest arrived either last or this timeslot to partitionarrived
			partitionArrived.put(partitionId, partitionArrivedState.get(partitionId));

			// need to check whether processed is valid
			if (partitionProcessedDelta.containsKey(partitionId)) {
				partitionProcessedState.put(partitionId,
					partitionProcessedState.getOrDefault(partitionId, 0l)+partitionProcessedDelta.get(partitionId));
				partitionProcessed.put(partitionId, partitionProcessedState.get(partitionId));
				// if this time processed is bigger than arrived, it means there must be some arrival
				if (partitionArrived.get(partitionId) < partitionProcessed.get(partitionId)) {
					partitionArrived.put(partitionId, partitionProcessed.get(partitionId));
				}
				partitionValid.put(partitionId, true);
			} else {
				partitionValid.put(partitionId, false);
			}
		}

		metrics.put("Arrived", partitionArrived);
		metrics.put("Processed", partitionProcessed);
		metrics.put("Utilization", executorUtilization);
		metrics.put("ServiceRate", executorServiceRate);
		metrics.put("Validity", partitionValid);

		return metrics;
	}

	public JobVertexID getVertexId() {
		return vertexID;
	}

	public String getPartitionId(String keyGroup) {
		return  keyGroup.split(":")[0];
//		return  "Partition " + keyGroup.split(":")[0];
	}

	public void retrievePartitionArrived(String[] ratesLine, HashMap<String, Long> partitionArrivedDelta,
										 HashMap<String, Long> workerTimeStamp, HashMap<String, Long> workerCurTimeStamp) {
		// TODO: need to consider multiple upstream tasks, we need to sum values from different upstream tasks
		if (!ratesLine[11].equals("0")) {
			long timestamp = Long.valueOf(ratesLine[13]);
			String workerId = ratesLine[1];

			String[] keyGroupsArrived = ratesLine[11].split("&");
			for (String keyGroup : keyGroupsArrived) {
				String partition = getPartitionId(keyGroup);
				long arrived = Long.valueOf(keyGroup.split(":")[1]);
				partitionArrivedDelta.put(partition,
					partitionArrivedDelta.getOrDefault(partition, 0l) + arrived);
			}
		}
	}

	public void retrievePartitionProcessed(String[] ratesLine, HashMap<String, Long> partitionProcessedDelta,
										   HashMap<String, Double> executorUtilization, HashMap<String, Double> executorServiceRate,
										   HashMap<String, Long> workerTimeStamp, HashMap<String, Long> workerCurTimeStamp) {
		// keygroups processed
		if (!ratesLine[12].equals("0")) {
			long timestamp = Long.valueOf(ratesLine[13]);
			String workerId = ratesLine[1];
			String executorId = workerId.split("-")[1];

			// utilization of executor
			if (Integer.valueOf(executorId) == JobRescalePartitionAssignment.UNUSED_SUBTASK) {
				return;
			}

			String[] keyGroupsProcessed = ratesLine[12].split("&");
			int actual_processed = 0;
			for (String keyGroup : keyGroupsProcessed) {
				String partition = getPartitionId(keyGroup);
				long processed = Long.valueOf(keyGroup.split(":")[1]);
				partitionProcessedDelta.put(partition,
					partitionProcessedDelta.getOrDefault(partition, 0l)+processed);
				actual_processed += processed;
			}

			if (Double.valueOf(ratesLine[10]) > 0) {
				executorUtilization.put(executorId, Double.valueOf(ratesLine[10]));
			}
			if (Double.valueOf(ratesLine[3]) > 0) {
				double serviceRate = Double.valueOf(ratesLine[3]);
				executorServiceRate.put(executorId, serviceRate);
			}

//			System.out.println("Executor id: " + ratesLine[1] + " utilization: " + ratesLine[10] + " processed: " + ratesLine[8]
//				+ " true rate: " + ratesLine[3] + " observed rate: " + ratesLine[5]);
//			System.out.println("actual processed: " + actual_processed + " records in: " + ratesLine[8] + " partition processed: " + ratesLine[12]);
		}
	}
}
