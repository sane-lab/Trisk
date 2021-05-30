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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

public abstract class JobGraphRescaler {

	protected final JobGraph jobGraph;
	protected final ClassLoader userCodeLoader;

	public JobGraphRescaler(JobGraph jobGraph, ClassLoader userCodeLoader) {
		this.jobGraph = jobGraph;
		this.userCodeLoader = userCodeLoader;
	}

	public abstract void rescale(JobVertexID id, int newParallelism, Map<Integer, List<Integer>> partitionAssignment, List<JobVertexID> involvedUpstream, List<JobVertexID> involvedDownstream);

	public abstract void repartition(JobVertexID id, Map<Integer, List<Integer>> partitionAssignment, List<JobVertexID> involvedUpstream, List<JobVertexID> involvedDownstream);

	public abstract String print(Configuration config);

	public static JobGraphRescaler instantiate(JobGraph jobGraph, ClassLoader userCodeLoader) {
		try {
			Class<? extends JobGraphRescaler> jobRescaleClass = Class
				.forName(jobGraph.getJobRescalerClassName(), true, userCodeLoader).asSubclass(JobGraphRescaler.class);

			Constructor<? extends JobGraphRescaler> statelessCtor = jobRescaleClass.getConstructor(JobGraph.class, ClassLoader.class);

			return statelessCtor.newInstance(jobGraph, userCodeLoader);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

//	public static JobGraphRescaler instantiate(ClassLoader userCodeLoader) throws Exception {
//		Class<? extends JobGraphRescaler> jobRescaleClass = Class
//			.forName("org.apache.flink.streaming.api.graph.StreamJobRescale", true, userCodeLoader).asSubclass(JobGraphRescaler.class);
//
//		Constructor<? extends JobGraphRescaler> statelessCtor = jobRescaleClass.getConstructor(JobGraph.class, ClassLoader.class);
//
//		return statelessCtor.newInstance(null, userCodeLoader);
//	}
}
