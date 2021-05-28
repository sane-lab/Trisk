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

package org.apache.flink.runtime.controlplane.streammanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.controlplane.ExecutionPlanAndJobGraphUpdaterFactory;
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;

import java.util.concurrent.CompletableFuture;

/**
 * rpc gateway interface
 */
public interface StreamManagerGateway extends FencedRpcGateway<StreamManagerId> {

	/**
	 * Register a {@link JobMaster} at the resource manager.
	 *
	 * @param jobMasterId         The fencing token for the JobMaster leader
	 * @param jobMasterResourceId The resource ID of the JobMaster that registers
	 * @param jobMasterAddress    The address of the JobMaster that registers
	 * @param jobId               The Job ID of the JobMaster that registers
	 * @param timeout             Timeout for the future to complete
	 * @return Future registration response
	 */
	CompletableFuture<RegistrationResponse> registerJobManager(
		JobMasterId jobMasterId,
		ResourceID jobMasterResourceId,
		String jobMasterAddress,
		JobID jobId,
		@RpcTimeout Time timeout);

	/**
	 * Disconnects the job manager from the stream manager because of the given cause.
	 *
	 * @param jobId The Job ID of the job manager
	 * @param cause the disconnection between the job manager and the stream manager
	 */
	void disconnectJobManager(
		final JobID jobId,
		final Exception cause);

	/**
	 * The notification from the JobManager that changes completed:
	 * Maybe 1. Assign states for repartition, 2. Rescale and assign states
	 * @param targetVertexID the JobVertexID of target vertex
	 */
	void streamSwitchCompleted(JobVertexID targetVertexID);

	/**
	 * This method is called whenever the status of the job changes.
	 *
	 * @param jobId         The ID of the job.
	 * @param newJobStatus  The status the job switched to.
	 * @param timestamp     The timestamp when the status transition occurred.
	 * @param error         In case the job status switches to a failure state, this is the
	 *                      exception that caused the failure.
	 */
	void jobStatusChanged(JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error, ExecutionPlan jobAbstraction);

	ExecutionPlanAndJobGraphUpdaterFactory getStreamRelatedInstanceFactory();

	boolean registerNewController(String controllerID, String className, String sourceCode);


}
