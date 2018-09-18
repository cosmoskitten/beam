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

package org.apache.beam.runners.flink;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class FlinkExecutionEnvironmentsTest {

  @Test
  public void shouldSetParallelismBatch() {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setRunner(TestFlinkRunner.class);
    options.setParallelism(42);

    ExecutionEnvironment bev =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(options.getParallelism(), is(42));
    assertThat(bev.getParallelism(), is(42));
  }

  @Test
  public void shouldSetParallelismStreaming() {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setRunner(TestFlinkRunner.class);
    options.setParallelism(42);

    StreamExecutionEnvironment sev =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(options.getParallelism(), is(42));
    assertThat(sev.getParallelism(), is(42));
  }

  @Test
  public void shouldPickupDefaultParallelismBatch() {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setRunner(TestFlinkRunner.class);

    ExecutionEnvironment bev =
        FlinkExecutionEnvironments.createBatchExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(options.getParallelism(), is(LocalStreamEnvironment.getDefaultLocalParallelism()));
    assertThat(bev.getParallelism(), is(LocalStreamEnvironment.getDefaultLocalParallelism()));
  }

  @Test
  public void shouldPickupDefaultParallelismStreaming() {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setRunner(TestFlinkRunner.class);

    StreamExecutionEnvironment sev =
        FlinkExecutionEnvironments.createStreamExecutionEnvironment(
            options, Collections.emptyList());

    assertThat(options.getParallelism(), is(LocalStreamEnvironment.getDefaultLocalParallelism()));
    assertThat(sev.getParallelism(), is(LocalStreamEnvironment.getDefaultLocalParallelism()));
  }
}
