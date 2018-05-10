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

import static com.google.common.base.Preconditions.checkArgument;

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
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.function.ThrowingConsumer;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

/** The Java SDK Harness implementation of the {@link Window.Assign} primitive. */
public class AssignWindowsRunner<T, W extends BoundedWindow> {

  /** A {@link PTransformRunnerFactory} to create runners for {@link Window.Assign} transforms. */
  public static class Factory implements PTransformRunnerFactory<AssignWindowsRunner> {
    @Override
    public AssignWindowsRunner<?, ?> createRunnerForPTransform(
        PipelineOptions pipelineOptions,
        BeamFnDataClient beamFnDataClient,
        BeamFnStateClient beamFnStateClient,
        String pTransformId,
        PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, PCollection> pCollections,
        Map<String, Coder> coders,
        Map<String, WindowingStrategy> windowingStrategies,
        Multimap<String, FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers,
        Consumer<ThrowingRunnable> addStartFunction,
        Consumer<ThrowingRunnable> addFinishFunction)
        throws IOException {
      checkArgument(
          PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN.equals(pTransform.getSpec().getUrn()));
      checkArgument(pTransform.getInputsCount() == 1, "Expected only one input");
      checkArgument(pTransform.getOutputsCount() == 1, "Expected only one output");
      WindowIntoPayload payload = WindowIntoPayload.parseFrom(pTransform.getSpec().getPayload());

      WindowFn<?, ?> windowFn =
          WindowingStrategyTranslation.windowFnFromProto(payload.getWindowFn());

      String outputId = Iterables.getOnlyElement(pTransform.getOutputsMap().values());
      Collection<FnDataReceiver<WindowedValue<?>>> outputConsumers =
          pCollectionIdsToConsumers.get(outputId);
      AssignWindowsRunner<?, ?> runner = AssignWindowsRunner.create(windowFn, outputConsumers);

      String inputId = Iterables.getOnlyElement(pTransform.getInputsMap().values());
      pCollectionIdsToConsumers.put(inputId, runner.asReceiver());
      return runner;
    }
  }

  /** A registrar which provides a factory to handle Java {@link WindowFn WindowFns}. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new Factory());
    }
  }
  ////////////////////////////////////////////////////////////////////////////////////////////////

  public static <T, W extends BoundedWindow> AssignWindowsRunner<T, W> create(
      WindowFn<? super T, W> windowFn,
      Collection<FnDataReceiver<WindowedValue<?>>> fnDataReceivers) {
    ThrowingConsumer<WindowedValue<T>> consumer =
        value -> {
          for (FnDataReceiver receiver : fnDataReceivers) {
            ((FnDataReceiver<WindowedValue<T>>) receiver).accept(value);
          }
        };
    // Safe contravariant cast
    WindowFn<T, W> typedWindowFn = (WindowFn<T, W>) windowFn;
    return new AssignWindowsRunner<>(typedWindowFn, consumer);
  }

  private final WindowFn<T, W> windowFn;
  private final ThrowingConsumer<WindowedValue<T>> outputConsumer;

  private AssignWindowsRunner(
      WindowFn<T, W> windowFn, ThrowingConsumer<WindowedValue<T>> outputConsumer) {
    this.windowFn = windowFn;
    this.outputConsumer = outputConsumer;
  }

  public void assignWindows(WindowedValue<T> compressedInput) throws Exception {
    for (WindowedValue<T> explodedInput : compressedInput.explodeWindows()) {
      WindowFn<T, W>.AssignContext ctxt =
          windowFn.new AssignContext() {
            @Override
            public T element() {
              return explodedInput.getValue();
            }

            @Override
            public Instant timestamp() {
              return explodedInput.getTimestamp();
            }

            @Override
            public BoundedWindow window() {
              return Iterables.getOnlyElement(explodedInput.getWindows());
            }
          };
      Collection<W> windows = windowFn.assignWindows(ctxt);
      outputConsumer.accept(
          WindowedValue.of(
              explodedInput.getValue(),
              explodedInput.getTimestamp(),
              windows,
              explodedInput.getPane()));
    }
  }

  private FnDataReceiver<WindowedValue<?>> asReceiver() {
    return input -> assignWindows((WindowedValue<T>) input);
  }
}
