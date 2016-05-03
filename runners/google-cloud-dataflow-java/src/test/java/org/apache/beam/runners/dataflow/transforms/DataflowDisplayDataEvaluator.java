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
package org.apache.beam.runners.dataflow.transforms;

import org.apache.beam.runners.dataflow.DataflowPipelineRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.util.NoopCredentialFactory;
import org.apache.beam.sdk.util.NoopPathValidator;

import com.google.common.collect.Lists;

import java.io.File;

/**
 * Factory methods for creating {@link DisplayDataEvaluator} instances against the
 * {@link DataflowPipelineRunner}.
 */
final class DataflowDisplayDataEvaluator {
  /** Do not instantiate. */
  private DataflowDisplayDataEvaluator() {}

  /**
   * Create a {@link DisplayDataEvaluator} instance to evaluate pipeline display data against
   * the {@link DataflowPipelineRunner}.
   *
   * @param tempLocation Temp location for {@link DataflowPipelineOptions#setTempLocation(String)}
   */
  static DisplayDataEvaluator create(File tempLocation) {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setProject("foobar");
    options.setTempLocation(tempLocation.getAbsolutePath());
    options.setFilesToStage(Lists.<String>newArrayList());

    options.as(DataflowPipelineDebugOptions.class).setPathValidatorClass(NoopPathValidator.class);
    options.as(GcpOptions.class).setCredentialFactoryClass(NoopCredentialFactory.class);

    return DisplayDataEvaluator.forRunner(DataflowPipelineRunner.class, options);
  }
}
