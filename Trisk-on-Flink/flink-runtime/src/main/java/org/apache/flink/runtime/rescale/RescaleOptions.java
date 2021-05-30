/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rescale;

import java.io.Serializable;

public class RescaleOptions implements Serializable {

	private final boolean scalingPartitions;

	private final boolean scalingGates;

	private final boolean repartition;

	private final boolean updateKeyGroupRange;

	public RescaleOptions(boolean scalingPartitions, boolean scalingGates, boolean repartition, boolean updateKeyGroupRange) {
		this.scalingPartitions = scalingPartitions;
		this.scalingGates = scalingGates;
		this.repartition = repartition;
		this.updateKeyGroupRange = updateKeyGroupRange;
	}

	public boolean isScalingPartitions() {
		return scalingPartitions;
	}

	public boolean isScalingGates() {
		return scalingGates;
	}

	public boolean isRepartition() {
		return repartition;
	}

	public boolean isUpdateKeyGroupRange() {
		return updateKeyGroupRange;
	}

	public final static RescaleOptions RESCALE_PARTITIONS_ONLY = new RescaleOptions(true, false, false, false);

	public final static RescaleOptions RESCALE_GATES_ONLY = new RescaleOptions(false, true, false, false);

	public final static RescaleOptions RESCALE_BOTH = new RescaleOptions(true, true, false, false);

	public final static RescaleOptions RESCALE_REDISTRIBUTE = new RescaleOptions(true, true, true, false);

	public final static RescaleOptions RESCALE_KEYGROUP_RANGE_ONLY = new RescaleOptions(false, false, false, true);

	@Override
	public int hashCode() {
		return (Boolean.hashCode(scalingPartitions) << 11 + Boolean.hashCode(scalingGates)) << 11 + Boolean.hashCode(repartition);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (obj instanceof RescaleOptions) {
			final RescaleOptions that = (RescaleOptions) obj;
			return this.scalingPartitions == that.scalingPartitions &&
				this.scalingGates == that.scalingGates &&
				this.repartition == that.repartition;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return "repartition: " + repartition + ", rescale partition: " + scalingPartitions + ", rescale gate: " + scalingGates;
	}
}
