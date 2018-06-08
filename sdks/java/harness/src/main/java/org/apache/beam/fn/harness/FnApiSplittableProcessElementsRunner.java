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

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.FnApiDoFnRunner.SideInputReaderImpl;
import org.apache.beam.fn.harness.FnApiDoFnRunner.SideInputSpec;
import org.apache.beam.fn.harness.control.BundleSplitListener;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleSplit.Application;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.SplittableProcessElementInvoker;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Runs the {@link PTransformTranslation#SPLITTABLE_PROCESS_ELEMENTS_URN} transform. */
public class FnApiSplittableProcessElementsRunner<InputT, RestrictionT, OutputT> {
  /** A registrar which provides a factory to handle Java {@link DoFn}s. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {
    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(PTransformTranslation.SPLITTABLE_PROCESS_ELEMENTS_URN, new Factory());
    }
  }

  static class Factory<InputT, RestrictionT, OutputT>
      implements PTransformRunnerFactory<
          FnApiSplittableProcessElementsRunner<InputT, RestrictionT, OutputT>> {

    @Override
    public FnApiSplittableProcessElementsRunner<InputT, RestrictionT, OutputT>
        createRunnerForPTransform(
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
            Consumer<ThrowingRunnable> addFinishFunction,
            BundleSplitListener splitListener) {
      FnApiDoFnRunner.Context<InputT, OutputT> context =
          new FnApiDoFnRunner.Context<>(
              pTransform, pCollections, coders, windowingStrategies, pCollectionIdsToConsumers);

      String inputId = Iterables.getOnlyElement(pTransform.getInputsMap().keySet());
      String inputPCollectionId = Iterables.getOnlyElement(pTransform.getInputsMap().values());
      PCollection inputPC = pCollections.get(inputPCollectionId);

      Coder<WindowedValue<KV<InputT, RestrictionT>>> windowedCoder =
          FullWindowedValueCoder.of(
              (Coder<KV<InputT, RestrictionT>>) context.inputCoder, context.windowCoder);

      @SuppressWarnings({"unchecked", "rawtypes"})
      FnApiSplittableProcessElementsRunner<InputT, RestrictionT, OutputT> runner =
          new FnApiSplittableProcessElementsRunner<>(
              pipelineOptions,
              beamFnStateClient,
              pTransformId,
              processBundleInstructionId,
              windowedCoder,
              context.doFn,
              (Collection<FnDataReceiver<WindowedValue<OutputT>>>)
                  (Collection) context.tagToConsumer.get(context.mainOutputTag),
              context.tagToConsumer,
              context.tagToSideInputSpecMap,
              splitListener,
              inputId);

      // Register the appropriate handlers.
      addStartFunction.accept(runner::startBundle);
      Iterable<String> mainInput =
          Sets.difference(
              pTransform.getInputsMap().keySet(), context.parDoPayload.getSideInputsMap().keySet());
      for (String localInputName : mainInput) {
        pCollectionIdsToConsumers.put(
            pTransform.getInputsOrThrow(localInputName),
            (FnDataReceiver)
                (FnDataReceiver<WindowedValue<KV<InputT, RestrictionT>>>) runner::processElement);
      }
      addFinishFunction.accept(runner::finishBundle);
      return runner;
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////

  private final PipelineOptions pipelineOptions;
  private final String ptransformId;
  private final String mainInputId;
  private final Coder<WindowedValue<KV<InputT, RestrictionT>>> inputCoder;
  private final DoFn<InputT, OutputT> doFn;
  private final Collection<FnDataReceiver<WindowedValue<OutputT>>> mainOutputConsumers;
  private final Multimap<TupleTag<?>, FnDataReceiver<WindowedValue<?>>> outputMap;
  private final SideInputReader sideInputReader;
  private final Map<StateKey, Object> stateKeyObjectCache;
  private final BundleSplitListener splitListener;
  private final DoFnInvoker<InputT, OutputT> doFnInvoker;
  private final ScheduledExecutorService executor;

  FnApiSplittableProcessElementsRunner(
      PipelineOptions pipelineOptions,
      BeamFnStateClient beamFnStateClient,
      String ptransformId,
      Supplier<String> processBundleInstructionId,
      Coder<WindowedValue<KV<InputT, RestrictionT>>> inputCoder,
      DoFn<InputT, OutputT> doFn,
      Collection<FnDataReceiver<WindowedValue<OutputT>>> mainOutputConsumers,
      Multimap<TupleTag<?>, FnDataReceiver<WindowedValue<?>>> outputMap,
      Map<TupleTag<?>, SideInputSpec> sideInputSpecMap,
      BundleSplitListener splitListener,
      String mainInputId) {
    this.pipelineOptions = pipelineOptions;
    this.ptransformId = ptransformId;
    this.mainInputId = mainInputId;
    this.inputCoder = inputCoder;
    this.doFn = doFn;
    this.mainOutputConsumers = mainOutputConsumers;
    this.outputMap = outputMap;
    this.stateKeyObjectCache = new HashMap<>();
    this.sideInputReader =
        new SideInputReaderImpl(
            stateKeyObjectCache,
            sideInputSpecMap,
            beamFnStateClient,
            ptransformId,
            processBundleInstructionId);
    this.splitListener = splitListener;
    this.doFnInvoker = DoFnInvokers.invokerFor(doFn);
    this.doFnInvoker.invokeSetup();
    this.executor = Executors.newSingleThreadScheduledExecutor();
  }

  public void startBundle() {
    doFnInvoker.invokeStartBundle(
        doFn.new StartBundleContext() {
          @Override
          public PipelineOptions getPipelineOptions() {
            return pipelineOptions;
          }
        });
  }

  public void processElement(WindowedValue<KV<InputT, RestrictionT>> elem) {
    processElementTyped(elem);
  }

  private <PositionT, TrackerT extends RestrictionTracker<RestrictionT, PositionT>>
      void processElementTyped(WindowedValue<KV<InputT, RestrictionT>> elem) {
    /* These members are only valid during {@link #processElement} and are null otherwise. */
    WindowedValue<InputT> element = elem.withValue(elem.getValue().getKey());
    TrackerT tracker = doFnInvoker.invokeNewTracker(elem.getValue().getValue());
    OutputAndTimeBoundedSplittableProcessElementInvoker<
            InputT, OutputT, RestrictionT, PositionT, TrackerT>
        processElementInvoker =
            new OutputAndTimeBoundedSplittableProcessElementInvoker<>(
                doFn,
                pipelineOptions,
                new OutputWindowedValue<OutputT>() {
                  @Override
                  public void outputWindowedValue(
                      OutputT output,
                      Instant timestamp,
                      Collection<? extends BoundedWindow> windows,
                      PaneInfo pane) {
                    outputTo(
                        mainOutputConsumers, WindowedValue.of(output, timestamp, windows, pane));
                  }

                  @Override
                  public <AdditionalOutputT> void outputWindowedValue(
                      TupleTag<AdditionalOutputT> tag,
                      AdditionalOutputT output,
                      Instant timestamp,
                      Collection<? extends BoundedWindow> windows,
                      PaneInfo pane) {
                    Collection<FnDataReceiver<WindowedValue<AdditionalOutputT>>> consumers =
                        (Collection) outputMap.get(tag);
                    if (consumers == null) {
                      throw new IllegalArgumentException(
                          String.format("Unknown output tag %s", tag));
                    }
                    outputTo(consumers, WindowedValue.of(output, timestamp, windows, pane));
                  }
                },
                sideInputReader,
                executor,
                10000,
                Duration.standardSeconds(10));

    SplittableProcessElementInvoker<InputT, OutputT, RestrictionT, TrackerT>.Result result =
        processElementInvoker.invokeProcessElement(doFnInvoker, element, tracker);
    if (result.getContinuation().shouldResume()) {
      WindowedValue<KV<InputT, RestrictionT>> primary =
          element.withValue(KV.of(element.getValue(), tracker.currentRestriction()));
      WindowedValue<KV<InputT, RestrictionT>> residual =
          element.withValue(KV.of(element.getValue(), result.getResidualRestriction()));
      byte[] primaryBytes, residualBytes;
      try {
        primaryBytes = CoderUtils.encodeToByteArray(inputCoder, primary);
        residualBytes = CoderUtils.encodeToByteArray(inputCoder, residual);
      } catch (CoderException e) {
        throw new RuntimeException(e);
      }
      Application primaryApplication =
          Application.newBuilder()
              .setPtransformId(ptransformId)
              .setInputId(mainInputId)
              .setElement(ByteString.copyFrom(primaryBytes))
              .build();
      Application residualApplication =
          Application.newBuilder()
              .setPtransformId(ptransformId)
              .setInputId(mainInputId)
              .setElement(ByteString.copyFrom(residualBytes))
              .build();
      splitListener.split(
          ptransformId,
          ImmutableList.of(primaryApplication),
          ImmutableList.of(residualApplication));
    }
  }

  public void finishBundle() {
    doFnInvoker.invokeFinishBundle(
        doFn.new FinishBundleContext() {
          @Override
          public PipelineOptions getPipelineOptions() {
            return pipelineOptions;
          }

          @Override
          public void output(OutputT output, Instant timestamp, BoundedWindow window) {
            throw new UnsupportedOperationException();
          }

          @Override
          public <T> void output(
              TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
            throw new UnsupportedOperationException();
          }
        });
    stateKeyObjectCache.clear();
  }

  /** Outputs the given element to the specified set of consumers wrapping any exceptions. */
  private <T> void outputTo(
      Collection<FnDataReceiver<WindowedValue<T>>> consumers, WindowedValue<T> output) {
    try {
      for (FnDataReceiver<WindowedValue<T>> consumer : consumers) {
        consumer.accept(output);
      }
    } catch (Throwable t) {
      throw UserCodeException.wrap(t);
    }
  }
}
