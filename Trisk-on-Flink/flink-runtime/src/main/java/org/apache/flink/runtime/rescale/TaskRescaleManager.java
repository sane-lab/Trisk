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

package org.apache.flink.runtime.rescale;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class TaskRescaleManager {

	private static final Logger LOG = LoggerFactory.getLogger(TaskRescaleManager.class);

	private final JobID jobId;

	private final ExecutionAttemptID executionId;

	private final String taskNameWithSubtaskAndId;

	private final TaskActions taskActions;

//	private final NetworkEnvironment network;

	private final IOManager ioManager;

	private final TaskMetricGroup metrics;

	private final ResultPartitionConsumableNotifier resultPartitionConsumableNotifier;

	private volatile TaskRescaleMeta rescaleMeta;

	private volatile ResultPartition[] storedOldWriterCopies;

	public TaskRescaleManager(
			JobID jobId,
			ExecutionAttemptID executionId,
			String taskNameWithSubtaskAndId,
			TaskActions taskActions,
//			NetworkEnvironment network,
			IOManager ioManager,
			TaskMetricGroup metrics,
			ResultPartitionConsumableNotifier notifier) {

		this.jobId = checkNotNull(jobId);
		this.executionId = checkNotNull(executionId);
		this.taskNameWithSubtaskAndId = checkNotNull(taskNameWithSubtaskAndId);
		this.taskActions = checkNotNull(taskActions);
//		this.network = checkNotNull(network);
		this.ioManager = checkNotNull(ioManager);
		this.metrics = checkNotNull(metrics);
		this.resultPartitionConsumableNotifier = checkNotNull(notifier);
	}

	public void prepareRescaleMeta(
			RescaleID rescaleId,
			RescaleOptions rescaleOptions,
			Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
			Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {

		TaskRescaleMeta meta = new TaskRescaleMeta(
			rescaleId,
			rescaleOptions,
			resultPartitionDeploymentDescriptors,
			inputGateDeploymentDescriptors);

		long timeStart = System.currentTimeMillis();
		while (rescaleMeta != null) {
			if (System.currentTimeMillis() - timeStart > 1000) {
				throw new IllegalStateException("One rescaling is in process, cannot prepare another rescaleMeta for " + taskNameWithSubtaskAndId);
			}
		}
		rescaleMeta = meta;
	}

	public boolean isScalingTarget() {
		return rescaleMeta != null;
	}

	public boolean isScalingPartitions() {
		return rescaleMeta.getRescaleOptions().isScalingPartitions();
	}

	public boolean isScalingGates() {
		return rescaleMeta.getRescaleOptions().isScalingGates();
	}

	public void createNewResultPartitions() throws IOException {
		int counter = 0;
		for (ResultPartitionDeploymentDescriptor desc : rescaleMeta.getResultPartitionDeploymentDescriptors()) {

		}
	}

	public ResultPartitionWriter[] substituteResultPartitions(ResultPartitionWriter[] oldWriters) {
		ResultPartitionWriter[] oldWriterCopies = Arrays.copyOf(oldWriters, oldWriters.length);

		for (int i = 0; i < oldWriters.length; i++) {
			oldWriters[i] = rescaleMeta.getNewPartitions(i);
		}

		return oldWriterCopies;
	}

	// We cannot do it immediately because downstream's gate is still polling from the old partitions (barrier haven't pass to downstream)
	// so we store the oldWriterCopies and unregister them in next scaling.
	public void unregisterPartitions(ResultPartition[] oldWriterCopies) {

	}

	public void substituteInputGateChannels(SingleInputGate inputGate) throws IOException, InterruptedException {
		checkNotNull(rescaleMeta, "rescale component cannot be null");

		throw new IllegalArgumentException("substituteInputGateChannels is not suppported now.");
	}

	private void createChannel(SingleInputGate inputGate) {
		throw new IllegalArgumentException("createChannel is not suppported now.");
	}

	public void finish() {
		this.rescaleMeta = null;
		LOG.info("++++++ taskRescaleManager finish, set meta to null for task " + taskNameWithSubtaskAndId);
	}

	private static class TaskRescaleMeta {
		private final RescaleID rescaleId;
		private final RescaleOptions rescaleOptions;

		private final Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors;
		private final Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors;

		private final ResultPartition[] newPartitions;

		TaskRescaleMeta(
				RescaleID rescaleId,
				RescaleOptions rescaleOptions,
				Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
				Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {

			this.rescaleId = checkNotNull(rescaleId);
			this.rescaleOptions = checkNotNull(rescaleOptions);

			this.resultPartitionDeploymentDescriptors = checkNotNull(resultPartitionDeploymentDescriptors);
			this.inputGateDeploymentDescriptors = checkNotNull(inputGateDeploymentDescriptors);

			this.newPartitions = new ResultPartition[resultPartitionDeploymentDescriptors.size()];
		}

		public RescaleID getRescaleId() {
			return rescaleId;
		}

		public RescaleOptions getRescaleOptions() {
			return rescaleOptions;
		}

		public Collection<ResultPartitionDeploymentDescriptor> getResultPartitionDeploymentDescriptors() {
			return resultPartitionDeploymentDescriptors;
		}

		public Collection<InputGateDeploymentDescriptor> getInputGateDeploymentDescriptors() {
			return inputGateDeploymentDescriptors;
		}

		public ResultPartition getNewPartitions(int index) {
			checkState(index >= 0 && index < newPartitions.length, "given index out of boundary");

			return newPartitions[index];
		}

		public void addNewPartitions(int index, ResultPartition partition) {
			checkState(index >= 0 && index < newPartitions.length, "given index out of boundary");

			newPartitions[index] = partition;
		}

		public InputGateDeploymentDescriptor getMatchedInputGateDescriptor(SingleInputGate gate) {
			return null;
		}
	}
}
