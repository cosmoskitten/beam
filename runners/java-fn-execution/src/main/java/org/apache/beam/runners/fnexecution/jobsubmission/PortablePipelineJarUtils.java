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
package org.apache.beam.runners.fnexecution.jobsubmission;

/** Contains common code for writing and reading portable pipeline jars. */
public class PortablePipelineJarUtils {
  private PortablePipelineJarUtils() {}

  static final String PIPELINE_FILE_NAME = "beam-pipeline.textproto";
  static final String PIPELINE_OPTIONS_FILE_NAME = "beam-pipeline-options.textproto";
  static final String ARTIFACT_MANIFEST_NAME = "beam-artifact-manifest.json";
  static final String ARTIFACT_FOLDER_NAME = "beam-artifact-staging";
}
