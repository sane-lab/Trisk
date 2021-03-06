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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;

/**
 * An {@link InputStatus} indicates one input state which might be currently
 * available, not available or already finished. It is returned while calling
 * {@link PushingAsyncDataInput#emitNext(DataOutput)}.
 */
@Internal
public enum InputStatus {

	/**
	 * Indicator that more data is available and the input can be called immediately again
	 * to emit more data.
	 */
	MORE_AVAILABLE,

	/**
	 * Indicator that no data is currently available, but more data will be available in the
	 * future again.
	 */
	NOTHING_AVAILABLE,

	/**
	 * Indicator that the input has reached the end of data.
	 */
	END_OF_INPUT,

	NEED_PAUSE;
}
