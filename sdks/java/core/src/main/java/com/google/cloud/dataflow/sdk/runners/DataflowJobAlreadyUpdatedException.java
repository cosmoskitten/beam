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
package com.google.cloud.dataflow.sdk.runners;

/**
 * An exception that is thrown if the existing job has already been updated within the Dataflow
 * service and is no longer able to be updated. The {@link DataflowPipelineJob} contained within
 * this exception contains information about the pre-existing updated job.
 */
public class DataflowJobAlreadyUpdatedException extends DataflowJobException {
  /**
   * Create a new {@code DataflowJobAlreadyUpdatedException} with the specified {@link
   * DataflowPipelineJob} and message.
   */
  public DataflowJobAlreadyUpdatedException(
      DataflowPipelineJob job, String message) {
    super(job, message, null);
  }
}
