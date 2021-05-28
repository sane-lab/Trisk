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

package org.apache.flink.runtime.util.profiling;

import org.apache.flink.configuration.Configuration;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

public class ReconfigurationProfiler {

	public static final String UPDATE_STATE = "update_state";
	public static final String UPDATE_KEY_MAPPING = "update_key_mapping";
	public static final String UPDATE_RESOURCE = "update_resource";

	// TODO: add breakdown profiling metrics such as sync time, update time, etc.
	private final Timer endToEndTimer;
	private final Timer syncTimer;
	private final Timer updateTimer;

	private final Map<String, Timer> otherTimers;
	OutputStream outputStream;

	public ReconfigurationProfiler(Configuration configuration) {
		try {
			String expDir = configuration.getString("trisk.exp.dir", "/data/flink");
			outputStream = new FileOutputStream(expDir + "/trisk/timer.output");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			outputStream = System.out;
		}
		endToEndTimer = new Timer("endToEndTimer", outputStream);
		syncTimer = new Timer("syncTimer");
		updateTimer = new Timer("updateTimer");
		otherTimers = new HashMap<>();
	}

	public void onReconfigurationStart() {
		endToEndTimer.startMeasure();
	}

	public void onSyncStart() {
		syncTimer.startMeasure();
	}

	public void onUpdateStart() {
		updateTimer.startMeasure();
	}

	public void onReconfigurationEnd() {
		endToEndTimer.endMeasure();
		endToEndTimer.finish();
	}

	public void onUpdateEnd() {
		updateTimer.endMeasure();
	}

	public void onOtherStart(String timerName) {
		Timer timer = this.otherTimers.get(timerName);
		if (timer == null) {
			timer = new Timer(timerName, outputStream);
			otherTimers.put(timerName, timer);
		}
		timer.startMeasure();
	}

	public void onOtherEnd(String timerName) {
		this.otherTimers.get(timerName).endMeasure();
	}

	private static class Timer {
		private final String timerName;
		private long startTime;
		private final OutputStreamDecorator outputStream;

		Timer(String timerName) {
			this(timerName, System.out);
		}

		Timer(String timerName, OutputStream outputStream) {
			this.timerName = timerName;
			this.startTime = 0;
			this.outputStream = new OutputStreamDecorator(outputStream);
		}

		public void startMeasure() {
			startTime = System.currentTimeMillis();
		}

		public void endMeasure() {
			checkState(startTime > 0, "++++++ Invalid invocation, startTime = 0.");
//			outputStream.println("end time: " + System.currentTimeMillis());
			outputStream.println("++++++" + timerName + " : " + (System.currentTimeMillis() - startTime) + "ms");
		}

		private void finish() {
			outputStream.println("cur time in ms: " + System.currentTimeMillis());
			outputStream.println("\n");
		}
	}
}


