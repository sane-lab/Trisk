package org.apache.flink.streaming.controlplane.rescale;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rescale.JobRescaleAction;
import org.apache.flink.runtime.rescale.JobRescalePartitionAssignment;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.apache.flink.streaming.controlplane.udm.ControlPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class RescaleActionConsumer implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(RescaleActionConsumer.class);

	private final ReconfigurationExecutor reconfigurationExecutor;
	private final ControlPolicy controlPolicy;

	private final Queue<JobRescaleAction.RescaleParamsWrapper> queue;

	private boolean isFinished;

	private volatile boolean isStop;

	public RescaleActionConsumer(ReconfigurationExecutor reconfigurationExecutor, ControlPolicy controlPolicy) {
		this.reconfigurationExecutor = reconfigurationExecutor;
		this.controlPolicy = controlPolicy;
		this.queue = new LinkedList<>();
	}

	@Override
	public void run() {
		while (!isStop) {
			synchronized (queue) {
				try {
					while (queue.isEmpty() && !isStop) {
						queue.wait(); // wait for new input
					}
					if (isStop) {
						return;
					}
					JobRescaleAction.RescaleParamsWrapper wrapper = queue.poll();
					if (wrapper != null) {
						isFinished = false;
//						rescaleAction.parseParams(wrapper);
//						primitiveInstruction.rescaleStreamJob(wrapper);
						Map<Integer, List<Integer>> partitionAssignment = wrapper.jobRescalePartitionAssignment.getPartitionAssignment();
//						List<List<Integer>> keyStateAllocation = new ArrayList<>(
//							partitionAssignment.values());
//						for(Integer key: partitionAssignment.keySet()){
//							keyStateAllocation.add(key, partitionAssignment.get(key));
//						}
						// todo should we use JobVertexID in user defined control policy?
						reconfigurationExecutor.rescale(-1, wrapper.newParallelism, partitionAssignment, controlPolicy);

						while (!isFinished && !isStop) {
							queue.wait(); // wait for finish
						}
						Thread.sleep(1000); // 30ms delay for fully deployment
					}
				} catch (Exception e) {
					e.printStackTrace();
					LOG.error("++++++ RescaleActionQueue err: ", e);
					return;
				}
			}
		}
	}

	public void put(
		JobRescaleAction.ActionType type,
		JobVertexID vertexID,
		int newParallelism,
		JobRescalePartitionAssignment jobRescalePartitionAssignment) {

		put(new JobRescaleAction.RescaleParamsWrapper(type, vertexID, newParallelism, jobRescalePartitionAssignment));
	}

	public void put(JobRescaleAction.RescaleParamsWrapper wrapper) {
		synchronized (queue) {
			queue.offer(wrapper);
			queue.notify();
		}
	}

	public void notifyFinished() {
		synchronized (queue) {
			isFinished = true;
			queue.notify();
		}
	}

	public void stopGracefully() {
		synchronized (queue) {
			isStop = true;
			queue.notify();
		}
	}
}


