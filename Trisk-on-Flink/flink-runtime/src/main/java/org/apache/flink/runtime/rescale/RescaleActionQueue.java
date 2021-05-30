package org.apache.flink.runtime.rescale;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;

import static org.apache.flink.runtime.rescale.JobRescaleAction.RescaleParamsWrapper;

public class RescaleActionQueue extends Thread {

	private static final Logger LOG = LoggerFactory.getLogger(RescaleActionQueue.class);

	private final JobRescaleAction rescaleAction;

	private final Queue<RescaleParamsWrapper> queue;

	private boolean isFinished;

	private volatile boolean isStop;

	public RescaleActionQueue(JobRescaleAction rescaleAction) {
		this.rescaleAction = rescaleAction;
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
					RescaleParamsWrapper wrapper = queue.poll();
					if (wrapper != null) {
						isFinished = false;
						rescaleAction.parseParams(wrapper);

						while (!isFinished && !isStop) {
							queue.wait(); // wait for finish
						}
						sleep(1000); // 30ms delay for fully deployment
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

		put(new RescaleParamsWrapper(type, vertexID, newParallelism, jobRescalePartitionAssignment));
	}

	public void put(RescaleParamsWrapper wrapper) {
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
