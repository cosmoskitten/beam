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
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.fn.harness.control.BundleSplitListener;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.MultimapSideInput;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleSplit.Application;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.SplittableProcessElementInvoker;
import org.apache.beam.runners.core.construction.PCollectionViewTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
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
      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(
              RunnerApi.Components.newBuilder()
                  .putAllPcollections(pCollections)
                  .putAllCoders(coders)
                  .putAllWindowingStrategies(windowingStrategies)
                  .build());

      DoFn<InputT, OutputT> doFn;
      TupleTag<OutputT> mainOutputTag;

      ImmutableMap.Builder<TupleTag<?>, SideInputSpec> tagToSideInputSpecMap =
          ImmutableMap.builder();
      ParDoPayload parDoPayload;
      try {
        parDoPayload = ParDoPayload.parseFrom(pTransform.getSpec().getPayload());
        doFn = (DoFn) ParDoTranslation.getDoFn(parDoPayload);
        mainOutputTag = (TupleTag) ParDoTranslation.getMainOutputTag(parDoPayload);

        // Build the map from tag id to side input specification
        for (Map.Entry<String, RunnerApi.SideInput> entry :
            parDoPayload.getSideInputsMap().entrySet()) {
          String sideInputTag = entry.getKey();
          RunnerApi.SideInput sideInput = entry.getValue();
          checkArgument(
              Materializations.MULTIMAP_MATERIALIZATION_URN.equals(
                  sideInput.getAccessPattern().getUrn()),
              "This SDK is only capable of dealing with %s materializations "
                  + "but was asked to handle %s for PCollectionView with tag %s.",
              Materializations.MULTIMAP_MATERIALIZATION_URN,
              sideInput.getAccessPattern().getUrn(),
              sideInputTag);

          PCollection sideInputPCollection =
              pCollections.get(pTransform.getInputsOrThrow(sideInputTag));
          WindowingStrategy sideInputWindowingStrategy =
              rehydratedComponents.getWindowingStrategy(
                  sideInputPCollection.getWindowingStrategyId());
          tagToSideInputSpecMap.put(
              new TupleTag<>(entry.getKey()),
              SideInputSpec.create(
                  rehydratedComponents.getCoder(sideInputPCollection.getCoderId()),
                  sideInputWindowingStrategy.getWindowFn().windowCoder(),
                  PCollectionViewTranslation.viewFnFromProto(entry.getValue().getViewFn()),
                  PCollectionViewTranslation.windowMappingFnFromProto(
                      entry.getValue().getWindowMappingFn())));
        }
      } catch (IOException exn) {
        throw new IllegalArgumentException("Malformed ParDoPayload", exn);
      }

      ImmutableListMultimap.Builder<TupleTag<?>, FnDataReceiver<WindowedValue<?>>>
          tagToConsumerBuilder = ImmutableListMultimap.builder();
      for (Map.Entry<String, String> entry : pTransform.getOutputsMap().entrySet()) {
        tagToConsumerBuilder.putAll(
            new TupleTag<>(entry.getKey()), pCollectionIdsToConsumers.get(entry.getValue()));
      }
      ListMultimap<TupleTag<?>, FnDataReceiver<WindowedValue<?>>> tagToConsumer =
          tagToConsumerBuilder.build();

      String inputId = Iterables.getOnlyElement(pTransform.getInputsMap().keySet());
      String inputPCollectionId = Iterables.getOnlyElement(pTransform.getInputsMap().values());
      PCollection inputPC = pCollections.get(inputPCollectionId);

      Coder<WindowedValue<KV<InputT, RestrictionT>>> windowedCoder;
      try {
        Coder<KV<InputT, RestrictionT>> inputCoder =
            (Coder<KV<InputT, RestrictionT>>) rehydratedComponents.getCoder(inputPC.getCoderId());
        Coder<? extends BoundedWindow> windowCoder =
            rehydratedComponents
                .getWindowingStrategy(inputPC.getWindowingStrategyId())
                .getWindowFn()
                .windowCoder();
        windowedCoder = FullWindowedValueCoder.of(inputCoder, windowCoder);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      @SuppressWarnings({"unchecked", "rawtypes"})
      FnApiSplittableProcessElementsRunner<InputT, RestrictionT, OutputT> runner =
          new FnApiSplittableProcessElementsRunner<>(
              pipelineOptions,
              beamFnStateClient,
              pTransformId,
              inputId,
              windowedCoder,
              processBundleInstructionId,
              doFn,
              (Collection<FnDataReceiver<WindowedValue<OutputT>>>)
                  (Collection) tagToConsumer.get(mainOutputTag),
              tagToConsumer,
              tagToSideInputSpecMap.build(),
              splitListener);
      registerHandlers(
          runner,
          pTransform,
          parDoPayload.getSideInputsMap().keySet(),
          addStartFunction,
          addFinishFunction,
          pCollectionIdsToConsumers);
      return runner;
    }
  }

  private static <InputT, RestrictionT, OutputT> void registerHandlers(
      FnApiSplittableProcessElementsRunner<InputT, RestrictionT, OutputT> runner,
      PTransform pTransform,
      Set<String> sideInputLocalNames,
      Consumer<ThrowingRunnable> addStartFunction,
      Consumer<ThrowingRunnable> addFinishFunction,
      Multimap<String, FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers) {
    // Register the appropriate handlers.
    addStartFunction.accept(runner::startBundle);
    for (String localInputName :
        Sets.difference(pTransform.getInputsMap().keySet(), sideInputLocalNames)) {
      pCollectionIdsToConsumers.put(
          pTransform.getInputsOrThrow(localInputName),
          (FnDataReceiver)
              (FnDataReceiver<WindowedValue<KV<InputT, RestrictionT>>>) runner::processElement);
    }
    addFinishFunction.accept(runner::finishBundle);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////

  private final PipelineOptions pipelineOptions;
  private final BeamFnStateClient beamFnStateClient;
  private final String ptransformId;
  private final String mainInputId;
  private final Coder<WindowedValue<KV<InputT, RestrictionT>>> coder;
  private final Supplier<String> processBundleInstructionId;
  private final DoFn<InputT, OutputT> doFn;
  private final Collection<FnDataReceiver<WindowedValue<OutputT>>> mainOutputConsumers;
  private final Multimap<TupleTag<?>, FnDataReceiver<WindowedValue<?>>> outputMap;
  private final Map<TupleTag<?>, SideInputSpec> sideInputSpecMap;
  private final BundleSplitListener splitListener;
  private final Map<StateKey, Object> stateKeyObjectCache;
  private final DoFnInvoker<InputT, OutputT> doFnInvoker;
  private final ScheduledExecutorService executor;

  FnApiSplittableProcessElementsRunner(
      PipelineOptions pipelineOptions,
      BeamFnStateClient beamFnStateClient,
      String ptransformId,
      String mainInputId,
      Coder<WindowedValue<KV<InputT, RestrictionT>>> coder,
      Supplier<String> processBundleInstructionId,
      DoFn<InputT, OutputT> doFn,
      Collection<FnDataReceiver<WindowedValue<OutputT>>> mainOutputConsumers,
      Multimap<TupleTag<?>, FnDataReceiver<WindowedValue<?>>> outputMap,
      Map<TupleTag<?>, SideInputSpec> sideInputSpecMap,
      BundleSplitListener splitListener) {
    this.pipelineOptions = pipelineOptions;
    this.beamFnStateClient = beamFnStateClient;
    this.ptransformId = ptransformId;
    this.mainInputId = mainInputId;
    this.coder = coder;
    this.processBundleInstructionId = processBundleInstructionId;
    this.doFn = doFn;
    this.mainOutputConsumers = mainOutputConsumers;
    this.outputMap = outputMap;
    this.sideInputSpecMap = sideInputSpecMap;
    this.splitListener = splitListener;
    this.stateKeyObjectCache = new HashMap<>();
    this.doFnInvoker = DoFnInvokers.invokerFor(doFn);
    this.doFnInvoker.invokeSetup();
    this.executor = Executors.newSingleThreadScheduledExecutor();
  }

  public void startBundle() {
    doFnInvoker.invokeStartBundle(new StartBundleContext());
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
                new SideInputReader() {
                  @Nullable
                  @Override
                  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
                    return bindSideInputView(window, view.getTagInternal());
                  }

                  @Override
                  public <T> boolean contains(PCollectionView<T> view) {
                    return sideInputSpecMap.containsKey(view);
                  }

                  @Override
                  public boolean isEmpty() {
                    return sideInputSpecMap.isEmpty();
                  }
                },
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
        primaryBytes = CoderUtils.encodeToByteArray(coder, primary);
        residualBytes = CoderUtils.encodeToByteArray(coder, residual);
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
          ptransformId, Arrays.asList(primaryApplication), Arrays.asList(residualApplication));
    }
  }

  public void finishBundle() {
    doFnInvoker.invokeFinishBundle(new FinishBundleContext());
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

  /** Provides arguments for a {@link DoFnInvoker} for {@link DoFn.StartBundle @StartBundle}. */
  private class StartBundleContext extends DoFn<InputT, OutputT>.StartBundleContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {

    private StartBundleContext() {
      doFn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
      throw new UnsupportedOperationException();
    }

    @Override
    public State state(String stateId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException();
    }
  }

  /** Provides arguments for a {@link DoFnInvoker} for {@link DoFn.FinishBundle @FinishBundle}. */
  private class FinishBundleContext extends DoFn<InputT, OutputT>.FinishBundleContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {

    private FinishBundleContext() {
      doFn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
      throw new UnsupportedOperationException();
    }

    @Override
    public State state(String stateId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void output(OutputT output, Instant timestamp, BoundedWindow window) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * A specification for side inputs containing a value {@link Coder}, the window {@link Coder},
   * {@link ViewFn}, and the {@link WindowMappingFn}.
   *
   * @param <W>
   */
  @AutoValue
  abstract static class SideInputSpec<W extends BoundedWindow> {
    static <W extends BoundedWindow> SideInputSpec create(
        Coder<?> coder,
        Coder<W> windowCoder,
        ViewFn<?, ?> viewFn,
        WindowMappingFn<W> windowMappingFn) {
      return new AutoValue_FnApiSplittableProcessElementsRunner_SideInputSpec<>(
          coder, windowCoder, viewFn, windowMappingFn);
    }

    abstract Coder<?> getCoder();

    abstract Coder<W> getWindowCoder();

    abstract ViewFn<?, ?> getViewFn();

    abstract WindowMappingFn<W> getWindowMappingFn();
  }

  private <T, K, V> T bindSideInputView(BoundedWindow currentWindow, TupleTag<?> view) {
    SideInputSpec sideInputSpec = sideInputSpecMap.get(view);
    checkArgument(sideInputSpec != null, "Attempting to access unknown side input %s.", view);
    KvCoder<K, V> kvCoder = (KvCoder) sideInputSpec.getCoder();

    ByteString.Output encodedWindowOut = ByteString.newOutput();
    try {
      sideInputSpec
          .getWindowCoder()
          .encode(
              sideInputSpec.getWindowMappingFn().getSideInputWindow(currentWindow),
              encodedWindowOut);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    ByteString encodedWindow = encodedWindowOut.toByteString();

    StateKey.Builder cacheKeyBuilder = StateKey.newBuilder();
    cacheKeyBuilder
        .getMultimapSideInputBuilder()
        .setPtransformId(ptransformId)
        .setSideInputId(view.getId())
        .setWindow(encodedWindow);
    return (T)
        stateKeyObjectCache.computeIfAbsent(
            cacheKeyBuilder.build(),
            key ->
                sideInputSpec
                    .getViewFn()
                    .apply(
                        createMultimapSideInput(
                            view.getId(),
                            encodedWindow,
                            kvCoder.getKeyCoder(),
                            kvCoder.getValueCoder())));
  }

  private <K, V> MultimapSideInput<K, V> createMultimapSideInput(
      String sideInputId, ByteString encodedWindow, Coder<K> keyCoder, Coder<V> valueCoder) {

    return new MultimapSideInput<>(
        beamFnStateClient,
        processBundleInstructionId.get(),
        ptransformId,
        sideInputId,
        encodedWindow,
        keyCoder,
        valueCoder);
  }
}
