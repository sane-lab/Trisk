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

import org.apache.flink.util.AbstractID;

import javax.annotation.Nullable;
import java.util.UUID;

/**
 * The {@link StreamManagerGateway} fencing token.
 */
public class StreamManagerId extends AbstractID {

	private static final long serialVersionUID = -1065386276246562408L;

	/**
	 * Creates a StreamManagerId that takes the bits from the given UUID.
	 */
	public StreamManagerId(UUID uuid) {
		super(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
	}

	/**
	 * Generates a new random StreamManagerId.
	 */
	private StreamManagerId() {
		super();
	}

	/**
	 * Creates a UUID with the bits from this StreamManagerId.
	 */
	public UUID toUUID() {
		return new UUID(getUpperPart(), getLowerPart());
	}

	/**
	 * Generates a new random JobMasterId.
	 */
	public static StreamManagerId generate() {
		return new StreamManagerId();
	}

	/**
	 * If the given uuid is null, this returns null, otherwise a StreamManagerId that
	 * corresponds to the UUID, via {@link #StreamManagerId(UUID)}.
	 */
	public static StreamManagerId fromUuidOrNull(@Nullable UUID uuid) {
		return  uuid == null ? null : new StreamManagerId(uuid);
	}
}
