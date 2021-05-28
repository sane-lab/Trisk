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

package org.apache.flink.streaming.controlplane.streammanager;

import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.util.Preconditions;

public class StreamManagerRuntimeServices {

    private final JobLeaderIdService jobLeaderIdService;

    public StreamManagerRuntimeServices(JobLeaderIdService jobLeaderIdService) {
        this.jobLeaderIdService = Preconditions.checkNotNull(jobLeaderIdService);
    }

    public JobLeaderIdService getJobLeaderIdService() {
        return jobLeaderIdService;
    }

    public static StreamManagerRuntimeServices fromConfiguration(
            StreamManagerConfiguration configuration,
            HighAvailabilityServices highAvailabilityServices,
            ScheduledExecutor scheduledExecutor) throws Exception {
        final JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
                highAvailabilityServices,
                scheduledExecutor,
                configuration.getRpcTimeout());

        return new StreamManagerRuntimeServices(jobLeaderIdService);
    }
}
