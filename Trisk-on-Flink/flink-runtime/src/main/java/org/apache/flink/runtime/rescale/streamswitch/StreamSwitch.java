package org.apache.flink.runtime.rescale.streamswitch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rescale.controller.OperatorControllerListener;
import org.apache.flink.runtime.rescale.metrics.KafkaMetricsRetriever;
import org.apache.flink.runtime.rescale.metrics.StockMetricsRetriever;
import org.apache.flink.runtime.rescale.metrics.StreamSwitchMetricsRetriever;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public abstract class StreamSwitch extends Thread implements FlinkOperatorController {

	private static final Logger LOG = LoggerFactory.getLogger(StreamSwitch.class);

	protected OperatorControllerListener listener;

	protected StreamSwitchMetricsRetriever metricsRetriever;

	Map<String, List<String>> executorMapping;

	protected long migrationInterval, metricsRetreiveInterval;

	volatile boolean isMigrating;

	protected long lastMigratedTime;

	protected long startTime;

	protected int maxNumberOfExecutors;

	//Lock is used to avoid concurrent modify between updateModel() and changeImplemented()
	ReentrantLock updateLock;

	AtomicLong nextExecutorID;

	private volatile boolean isStopped;

	private String[] targetVertices;

	private Configuration config;

	private long timeIndex;

	private int numPartitions; // be used for metrics retriever, for initial metrics

	protected Map<String, Long> oeUnlockTime = new HashMap<>();


	@Override
	public void init(OperatorControllerListener listener, List<String> executors, List<String> partitions) {
		LOG.info("Initialize with executors: " + executors + "  partitions: " + partitions);

		isMigrating = false;
		lastMigratedTime = 0;
		updateLock = new ReentrantLock();
		timeIndex = 1;

		this.listener = listener;

		this.executorMapping = new HashMap<>();

		int numExecutors = executors.size();
		numPartitions = partitions.size();
		for (int executorId = 0; executorId < numExecutors; executorId++) {
			List<String> executorPartitions = new ArrayList<>();
			executorMapping.put(String.valueOf(executorId), executorPartitions);

			KeyGroupRange keyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
				numPartitions, numExecutors, executorId);
			for (int i = keyGroupRange.getStartKeyGroup(); i <= keyGroupRange.getEndKeyGroup(); i++) {
				executorPartitions.add(String.valueOf(i));
			}
		}

		this.nextExecutorID = new AtomicLong(numExecutors);
		this.listener.setup(executorMapping);
	}

	@Override
	public void initMetrics(JobGraph jobGraph, JobVertexID vertexID, Configuration config, int parallelism) {
		String app = config.getString("model.app", "others");
		if (app.equals("stock")) {
			this.metricsRetriever = new StockMetricsRetriever();
		} else {
			this.metricsRetriever = new KafkaMetricsRetriever();
		}
		this.metricsRetriever.init(jobGraph, vertexID, config, numPartitions, parallelism);
		// TODO: init job configurations can be placed into constructor.
		this.config = config;
		String verticesStr = config.getString("model.vertex", "c21234bcbf1e8eb4c61f1927190efebd");
		targetVertices = verticesStr.split(",");
		for (int i = 0; i < targetVertices.length; i++) {
			targetVertices[i] = targetVertices[i].trim();
		}
		migrationInterval = config.getLong("streamswitch.system.migration_interval", 5000l); //Scale-out takes some time
	}

	@Override
	public void run() {
		boolean isContain = false;
		for (String targetVertex : targetVertices) {
			if (metricsRetriever.getVertexId().equals(JobVertexID.fromHexString(targetVertex))) {
				isContain = true;
			}
		}
		if (!isContain) {
			return;
		}

		int metricsWarmupTime = config.getInteger("streamswitch.system.warmup_time", 20000);

		//Warm up phase
		warmUp(metricsWarmupTime);

		//Start running
		startTime = System.currentTimeMillis() - metricsRetreiveInterval; //current time is time index 1, need minus a interval to index 0
		while(!isStopped) {
			//Actual calculation, decision here
			updateLock.lock();
			try{
				work(timeIndex);
			}finally {
				updateLock.unlock();
			}

			//Calculate # of time slots we have to skip due to longer calculation
			long deltaT = System.currentTimeMillis() - startTime;
			long nextIndex = deltaT / metricsRetreiveInterval + 1;
			long sleepTime = nextIndex * metricsRetreiveInterval - deltaT;
			if(nextIndex > timeIndex + 1){
				LOG.info("Skipped to index:" + nextIndex + " from index:" + timeIndex + " deltaT=" + deltaT);
			}
			timeIndex = nextIndex;
			LOG.info("Sleep for " + sleepTime + "milliseconds");
			try{
				Thread.sleep(sleepTime);
			}catch (Exception e) {
				LOG.warn("Exception happens between run loop interval, ", e);
				isStopped = true;
			}
		}
		LOG.info("Stream switch stopped");
	}

	@Override
	public void stopGracefully() {
		isStopped = true;
	}

//	@Override
//	public synchronized void onChangeImplemented() {
//		if (isMigrating) {
//			isMigrating = false;
//		}
//	}

	//Need extend class to implement
	//Return true if migration is triggered
//	protected boolean updateModel(long time, Map<String, Object> metrics) {
//		return false;
//	}

	abstract void work(long timeIndex);

	@Override
	public void onForceRetrieveMetrics() {
		// TODO: consistency problem is omitted now, but should be considered later..
		//Actual calculation, decision here
//		work(timeIndex);
//		//Calculate # of time slots we have to skip due to longer calculation
//		long deltaT = System.currentTimeMillis() - startTime;
//		long nextIndex = deltaT / metricsRetreiveInterval + 1;
//		long sleepTime = nextIndex * metricsRetreiveInterval - deltaT;
//		if(nextIndex > timeIndex + 1){
//			LOG.info("Skipped to index:" + nextIndex + " from index:" + timeIndex + " deltaT=" + deltaT);
//		}
//		timeIndex = nextIndex;
//		LOG.info("Sleep for " + sleepTime + "milliseconds");
//		try{
//			Thread.sleep(sleepTime);
//		}catch (Exception e) {
//			LOG.warn("Exception happens between run loop interval, ", e);
//			isStopped = true;
//		}
	}

	private void warmUp(long metricsWarmupTime) {
		long warmUpStartTime = System.currentTimeMillis();
		//Warm up phase
		LOG.info("Warm up for " + metricsWarmupTime + " milliseconds...");
		do{
			long time = System.currentTimeMillis();
			if(time - warmUpStartTime > metricsWarmupTime){
				break;
			}
			else {
				try{
					Thread.sleep(500l);
				}catch (Exception e){
					LOG.error("Exception happens during warming up, ", e);
				}
			}
		}while(true);
		LOG.info("Warm up completed.");
	}

}
