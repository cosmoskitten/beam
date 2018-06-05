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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.MultimapSideInput;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.construction.PCollectionViewTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.sdk.coders.Coder;
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
import org.apache.beam.sdk.transforms.DoFnOutputReceivers;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * A {@link DoFnRunner} specific to integrating with the Fn Api. This is to remove the layers of
 * abstraction caused by StateInternals/TimerInternals since they model state and timer concepts
 * differently.
 */
public class FnApiSplittableProcessKeyedRunner<InputT, RestrictionT, OutputT> {
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
          FnApiSplittableProcessKeyedRunner<InputT, RestrictionT, OutputT>> {

    @Override
    public FnApiSplittableProcessKeyedRunner<InputT, RestrictionT, OutputT>
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
            Consumer<ThrowingRunnable> addFinishFunction) {

      DoFn<InputT, OutputT> doFn;
      TupleTag<OutputT> mainOutputTag;

      ImmutableMap.Builder<TupleTag<?>, SideInputSpec> tagToSideInputSpecMap =
          ImmutableMap.builder();
      ParDoPayload parDoPayload;
      try {
        RehydratedComponents rehydratedComponents =
            RehydratedComponents.forComponents(
                RunnerApi.Components.newBuilder()
                    .putAllCoders(coders)
                    .putAllWindowingStrategies(windowingStrategies)
                    .build());
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

      @SuppressWarnings({"unchecked", "rawtypes"})
      FnApiSplittableProcessKeyedRunner<InputT, RestrictionT, OutputT> runner =
          new FnApiSplittableProcessKeyedRunner<>(
              pipelineOptions,
              beamFnStateClient,
              pTransformId,
              processBundleInstructionId,
              doFn,
              (Collection<FnDataReceiver<WindowedValue<OutputT>>>)
                  (Collection) tagToConsumer.get(mainOutputTag),
              tagToConsumer,
              tagToSideInputSpecMap.build());
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
      FnApiSplittableProcessKeyedRunner<InputT, RestrictionT, OutputT> runner,
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
  private final Supplier<String> processBundleInstructionId;
  private final DoFn<InputT, OutputT> doFn;
  private final Collection<FnDataReceiver<WindowedValue<OutputT>>> mainOutputConsumers;
  private final Multimap<TupleTag<?>, FnDataReceiver<WindowedValue<?>>> outputMap;
  private final Map<TupleTag<?>, SideInputSpec> sideInputSpecMap;
  private final Map<StateKey, Object> stateKeyObjectCache;
  private final DoFnInvoker<InputT, OutputT> doFnInvoker;
  private final StartBundleContext startBundleContext;
  private final ProcessBundleContext processBundleContext;
  private final FinishBundleContext finishBundleContext;

  /** These members are only valid during {@link #processElement} and are null otherwise. */
  private WindowedValue<InputT> currentElement;

  private RestrictionTracker<RestrictionT, ?> currentTracker;
  private BoundedWindow currentWindow;

  FnApiSplittableProcessKeyedRunner(
      PipelineOptions pipelineOptions,
      BeamFnStateClient beamFnStateClient,
      String ptransformId,
      Supplier<String> processBundleInstructionId,
      DoFn<InputT, OutputT> doFn,
      Collection<FnDataReceiver<WindowedValue<OutputT>>> mainOutputConsumers,
      Multimap<TupleTag<?>, FnDataReceiver<WindowedValue<?>>> outputMap,
      Map<TupleTag<?>, SideInputSpec> sideInputSpecMap) {
    this.pipelineOptions = pipelineOptions;
    this.beamFnStateClient = beamFnStateClient;
    this.ptransformId = ptransformId;
    this.processBundleInstructionId = processBundleInstructionId;
    this.doFn = doFn;
    this.mainOutputConsumers = mainOutputConsumers;
    this.outputMap = outputMap;
    this.sideInputSpecMap = sideInputSpecMap;
    this.stateKeyObjectCache = new HashMap<>();
    this.doFnInvoker = DoFnInvokers.invokerFor(doFn);
    this.doFnInvoker.invokeSetup();
    this.startBundleContext = new StartBundleContext();
    this.processBundleContext = new ProcessBundleContext();
    this.finishBundleContext = new FinishBundleContext();
  }

  public void startBundle() {
    doFnInvoker.invokeStartBundle(startBundleContext);
  }

  public void processElement(WindowedValue<KV<InputT, RestrictionT>> elem) {
    try {
      currentWindow = Iterables.getOnlyElement(elem.getWindows());
      currentElement = elem.withValue(elem.getValue().getKey());
      currentTracker = doFnInvoker.invokeNewTracker(elem.getValue().getValue());
      doFnInvoker.invokeProcessElement(processBundleContext);
    } finally {
      currentWindow = null;
      currentElement = null;
      currentTracker = null;
    }
  }

  public void finishBundle() {
    doFnInvoker.invokeFinishBundle(finishBundleContext);

    // TODO: Support caching state data across bundle boundaries.
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

  /**
   * Provides arguments for a {@link DoFnInvoker} for {@link DoFn.ProcessElement @ProcessElement}.
   */
  private class ProcessBundleContext extends DoFn<InputT, OutputT>.ProcessContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {

    private ProcessBundleContext() {
      doFn.super();
    }

    @Override
    public BoundedWindow window() {
      return currentWindow;
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      return pane();
    }

    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      return element();
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return timestamp();
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
      return currentTracker;
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
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedReceiver(this, null);
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedMultiReceiver(this);
    }

    @Override
    public void output(OutputT output) {
      outputTo(
          mainOutputConsumers,
          WindowedValue.of(
              output, currentElement.getTimestamp(), currentWindow, currentElement.getPane()));
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      outputTo(
          mainOutputConsumers,
          WindowedValue.of(output, timestamp, currentWindow, currentElement.getPane()));
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      Collection<FnDataReceiver<WindowedValue<T>>> consumers = (Collection) outputMap.get(tag);
      if (consumers == null) {
        throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
      }
      outputTo(
          consumers,
          WindowedValue.of(
              output, currentElement.getTimestamp(), currentWindow, currentElement.getPane()));
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      Collection<FnDataReceiver<WindowedValue<T>>> consumers = (Collection) outputMap.get(tag);
      if (consumers == null) {
        throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
      }
      outputTo(
          consumers, WindowedValue.of(output, timestamp, currentWindow, currentElement.getPane()));
    }

    @Override
    public InputT element() {
      return currentElement.getValue();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      return bindSideInputView(view.getTagInternal());
    }

    @Override
    public Instant timestamp() {
      return currentElement.getTimestamp();
    }

    @Override
    public PaneInfo pane() {
      return currentElement.getPane();
    }

    @Override
    public void updateWatermark(Instant watermark) {
      throw new UnsupportedOperationException("TODO: Add support for SplittableDoFn");
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException();
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
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
      return new AutoValue_FnApiSplittableProcessKeyedRunner_SideInputSpec<>(
          coder, windowCoder, viewFn, windowMappingFn);
    }

    abstract Coder<?> getCoder();

    abstract Coder<W> getWindowCoder();

    abstract ViewFn<?, ?> getViewFn();

    abstract WindowMappingFn<W> getWindowMappingFn();
  }

  private <T, K, V> T bindSideInputView(TupleTag<?> view) {
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
