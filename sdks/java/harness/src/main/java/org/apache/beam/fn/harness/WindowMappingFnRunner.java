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

package org.apache.beam.fn.harness;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.beam.runners.core.construction.UrnUtils.validateCommonUrn;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.MultiplexingFnDataReceiver;
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.runners.core.construction.PCollectionViewTranslation;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps windows using a window mapping fn.
 */
public class WindowMappingFnRunner<InputT extends BoundedWindow, OutputT extends BoundedWindow> {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataReadRunner.class);
  static final String URN = validateCommonUrn("beam:transform:map_windows:v1");

  /**
   * A registrar which provides a factory to handle mapping main input windows onto side input
   * windows.
   */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(URN, new Factory());
    }
  }

  /** A factory for {@link BeamFnDataReadRunner}s. */
  static class Factory<InputT extends BoundedWindow, OutputT extends BoundedWindow>
      implements PTransformRunnerFactory<WindowMappingFnRunner<InputT, OutputT>> {

    @Override
    public WindowMappingFnRunner<InputT, OutputT> createRunnerForPTransform(
        PipelineOptions pipelineOptions,
        BeamFnDataClient beamFnDataClient,
        BeamFnStateClient beamFnStateClient,
        String pTransformId,
        PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, PCollection> pCollections,
        Map<String, RunnerApi.Coder> coders,
        Map<String, RunnerApi.WindowingStrategy> windowingStrategies,
        Multimap<String, FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers,
        Consumer<ThrowingRunnable> addStartFunction,
        Consumer<ThrowingRunnable> addFinishFunction) throws IOException {

      Collection<FnDataReceiver<WindowedValue<OutputT>>> consumers =
          (Collection) pCollectionIdsToConsumers.get(
              getOnlyElement(pTransform.getOutputsMap().values()));

      SdkFunctionSpec windowMappingFnPayload =
          SdkFunctionSpec.parseFrom(pTransform.getSpec().getPayload());
      WindowMappingFn<OutputT> windowMappingFn =
          (WindowMappingFn<OutputT>) PCollectionViewTranslation.windowMappingFnFromProto(
              windowMappingFnPayload);

      WindowMappingFnRunner<InputT, OutputT> runner =
          new WindowMappingFnRunner<>(
              windowMappingFn,
              MultiplexingFnDataReceiver.forConsumers(consumers));
      pCollectionIdsToConsumers.put(
          Iterables.getOnlyElement(pTransform.getInputsMap().values()),
          (FnDataReceiver) (FnDataReceiver<WindowedValue<InputT>>) runner::mapWindow);
      return runner;
    }
  }

  private final WindowMappingFn<OutputT> windowMappingFn;
  private final FnDataReceiver<WindowedValue<OutputT>> consumer;

  public WindowMappingFnRunner(
      WindowMappingFn<OutputT> windowMappingFn,
      FnDataReceiver<WindowedValue<OutputT>> consumer) {
    this.windowMappingFn = windowMappingFn;
    this.consumer = consumer;
  }

  public void mapWindow(WindowedValue<InputT> element) throws Exception {
    WindowedValue<OutputT> output =
        element.withValue(windowMappingFn.getSideInputWindow(element.getValue()));
    consumer.accept(output);
  }
}
