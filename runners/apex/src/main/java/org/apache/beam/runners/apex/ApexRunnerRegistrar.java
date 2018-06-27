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

package org.apache.beam.runners.apex;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar;

/**
 * Contains the {@link PipelineRunnerRegistrar} and {@link PipelineOptionsRegistrar} for the {@link
 * ApexRunner}.
 *
 * <p>{@link AutoService} will register Apex's implementations of the {@link PipelineRunner} and
 * {@link PipelineOptions} as available pipeline runner services.
 */
public final class ApexRunnerRegistrar {
  private ApexRunnerRegistrar() {}

  /** Registers the {@link ApexRunner}. */
  @AutoService(PipelineRunnerRegistrar.class)
  public static class Runner implements PipelineRunnerRegistrar {
    @Override
    public Iterable<Class<? extends PipelineRunner<?>>> getPipelineRunners() {
      return ImmutableList.of(ApexRunner.class, TestApexRunner.class);
    }
  }

  /** Registers the {@link ApexPipelineOptions}. */
  @AutoService(PipelineOptionsRegistrar.class)
  public static class Options implements PipelineOptionsRegistrar {
    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return ImmutableList.of(ApexPipelineOptions.class);
    }
  }
}
