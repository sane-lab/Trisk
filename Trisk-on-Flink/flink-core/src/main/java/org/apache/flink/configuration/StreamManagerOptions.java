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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Configuration options for the StreamManager.
 */
@PublicEvolving
public class StreamManagerOptions {

	/**
	 * The config parameter defining the network address to connect to
	 * for communication with the job manager.
	 *
	 * <p>This value is only interpreted in setups where a single StreamManager with static
	 * name or address exists (simple standalone setups, or container setups with dynamic
	 * service name resolution). It is not used in many high-availability setups, when a
	 * leader-election service (like ZooKeeper) is used to elect and discover the StreamManager
	 * leader from potentially multiple standby StreamManagers.
	 */
	@Documentation.Section({Documentation.Sections.COMMON_HOST_PORT, Documentation.Sections.ALL_JOB_MANAGER})
	public static final ConfigOption<String> ADDRESS =
		key("streammanager.rpc.address")
		.noDefaultValue()
		.withDescription("The config parameter defining the network address to connect to" +
			" for communication with the job manager." +
			" This value is only interpreted in setups where a single StreamManager with static" +
			" name or address exists (simple standalone setups, or container setups with dynamic" +
			" service name resolution). It is not used in many high-availability setups, when a" +
			" leader-election service (like ZooKeeper) is used to elect and discover the StreamManager" +
			" leader from potentially multiple standby StreamManagers.");

	/**
	 * The config parameter defining the network port to connect to
	 * for communication with the job manager.
	 *
	 * <p>Like {@link StreamManagerOptions#ADDRESS}, this value is only interpreted in setups where
	 * a single StreamManager with static name/address and port exists (simple standalone setups,
	 * or container setups with dynamic service name resolution).
	 * This config option is not used in many high-availability setups, when a
	 * leader-election service (like ZooKeeper) is used to elect and discover the StreamManager
	 * leader from potentially multiple standby StreamManagers.
	 */
	@Documentation.Section({Documentation.Sections.COMMON_HOST_PORT, Documentation.Sections.ALL_JOB_MANAGER})
	public static final ConfigOption<Integer> PORT =
		key("streammanager.rpc.port")
		.defaultValue(8025)
		.withDescription("The config parameter defining the network port to connect to" +
			" for communication with the job manager." +
			" Like " + ADDRESS.key() + ", this value is only interpreted in setups where" +
			" a single StreamManager with static name/address and port exists (simple standalone setups," +
			" or container setups with dynamic service name resolution)." +
			" This config option is not used in many high-availability setups, when a" +
			" leader-election service (like ZooKeeper) is used to elect and discover the StreamManager" +
			" leader from potentially multiple standby StreamManagers.");

	/**
	 * JVM heap size for the StreamManager with memory size.
	 */
	@Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
	public static final ConfigOption<String> JOB_MANAGER_HEAP_MEMORY =
		key("streammanager.heap.size")
		.defaultValue("1024m")
		.withDescription("JVM heap size for the StreamManager.");

	/**
	 * JVM heap size (in megabytes) for the StreamManager.
	 * @deprecated use {@link #JOB_MANAGER_HEAP_MEMORY}
	 */
	@Deprecated
	public static final ConfigOption<Integer> JOB_MANAGER_HEAP_MEMORY_MB =
		key("streammanager.heap.mb")
		.defaultValue(1024)
		.withDescription("JVM heap size (in megabytes) for the StreamManager.");

	/**
	 * The maximum number of prior execution attempts kept in history.
	 */
	@Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
	public static final ConfigOption<Integer> MAX_ATTEMPTS_HISTORY_SIZE =
		key("streammanager.execution.attempts-history-size")
			.defaultValue(16)
			.withDeprecatedKeys("job-manager.max-attempts-history-size")
			.withDescription("The maximum number of prior execution attempts kept in history.");

	/**
	 * This option specifies the failover strategy, i.e. how the job computation recovers from task failures.
	 *
	 * <p>The option "individual" is intentionally not included for its known limitations.
	 * It only works when all tasks are not connected, in which case the "region"
	 * failover strategy would also restart failed tasks individually.
	 * The new "region" strategy supersedes "individual" strategy and should always work.
	 */
	@Documentation.Section({Documentation.Sections.ALL_JOB_MANAGER, Documentation.Sections.EXPERT_FAULT_TOLERANCE})
	@Documentation.OverrideDefault("region")
	public static final ConfigOption<String> EXECUTION_FAILOVER_STRATEGY =
		key("streammanager.execution.failover-strategy")
			.defaultValue("full")
			.withDescription(Description.builder()
				.text("This option specifies how the job computation recovers from task failures. " +
					"Accepted values are:")
				.list(
					text("'full': Restarts all tasks to recover the job."),
					text("'region': Restarts all tasks that could be affected by the task failure. " +
						"More details can be found %s.",
						link(
							"../dev/task_failure_recovery.html#restart-pipelined-region-failover-strategy",
							"here"))
				).build());

	/**
	 * The location where the StreamManager stores the archives of completed jobs.
	 */
	@Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
	public static final ConfigOption<String> ARCHIVE_DIR =
		key("streammanager.archive.fs.dir")
			.noDefaultValue()
			.withDescription("Dictionary for StreamManager to store the archives of completed jobs.");

	/**
	 * The job store cache size in bytes which is used to keep completed
	 * jobs in memory.
	 */
	@Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
	public static final ConfigOption<Long> JOB_STORE_CACHE_SIZE =
		key("jobstore.cache-size")
		.defaultValue(50L * 1024L * 1024L)
		.withDescription("The job store cache size in bytes which is used to keep completed jobs in memory.");

	/**
	 * The time in seconds after which a completed job expires and is purged from the job store.
	 */
	@Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
	public static final ConfigOption<Long> JOB_STORE_EXPIRATION_TIME =
		key("jobstore.expiration-time")
		.defaultValue(60L * 60L)
		.withDescription("The time in seconds after which a completed job expires and is purged from the job store.");

	/**
	 * The max number of completed jobs that can be kept in the job store.
	 */
	@Documentation.Section(Documentation.Sections.ALL_JOB_MANAGER)
	public static final ConfigOption<Integer> JOB_STORE_MAX_CAPACITY =
		key("jobstore.max-capacity")
			.defaultValue(Integer.MAX_VALUE)
			.withDescription("The max number of completed jobs that can be kept in the job store.");
	// ---------------------------------------------------------------------------------------------

	private StreamManagerOptions() {
		throw new IllegalAccessError();
	}
}
