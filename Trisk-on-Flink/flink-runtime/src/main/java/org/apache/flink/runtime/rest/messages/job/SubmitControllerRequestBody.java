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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Request for submitting a job.
 *
 * <p>This request only contains the names of files that must be present on the server, and defines how these files are
 * interpreted.
 */
public final class SubmitControllerRequestBody implements RequestBody {

	public static final String CONTROLLER_CLASS_NAME = "className";
	private static final String CONTROLLER_FILE_NAME = "classFile";
	private static final String CONTROLLER_ID = "controllerID";

	@JsonProperty(CONTROLLER_CLASS_NAME)
	@Nonnull
	public final String controllerClassName;

	@JsonProperty(CONTROLLER_FILE_NAME)
	@Nonnull
	public final String classFile;

	@JsonProperty(CONTROLLER_ID)
	@Nullable
	public final String controllerID;

	@JsonCreator
	public SubmitControllerRequestBody(
		@Nullable @JsonProperty(CONTROLLER_CLASS_NAME) String controllerClassName,
		@Nullable @JsonProperty(CONTROLLER_FILE_NAME) String classFile,
		@Nullable @JsonProperty(CONTROLLER_ID) String controllerID) {
		this.controllerClassName = controllerClassName;
		this.classFile = classFile;
		this.controllerID = controllerID;
	}

	@Override
	public String toString() {
		return "SubmitControllerRequestBody{" +
			"controllerClassName='" + controllerClassName + '\'' +
			", classFile='" + classFile + '\'' +
			", controllerID='" + controllerID + '\'' +
			'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		SubmitControllerRequestBody that = (SubmitControllerRequestBody) o;
		return Objects.equals(controllerClassName, that.controllerClassName) &&
			Objects.equals(classFile, that.classFile) &&
			Objects.equals(controllerID, that.controllerID);
	}

	@Override
	public int hashCode() {
		return Objects.hash(controllerClassName, classFile, controllerID);
	}


}
