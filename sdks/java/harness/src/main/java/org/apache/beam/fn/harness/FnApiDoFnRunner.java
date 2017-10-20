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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.service.AutoService;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
import org.apache.beam.fn.harness.state.BagUserState;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.MultimapSideInput;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.ReadableStates;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateBinder;
import org.apache.beam.sdk.state.StateContext;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OnTimerContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.StateDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * A {@link DoFnRunner} specific to integrating with the Fn Api. This is to remove the layers
 * of abstraction caused by StateInternals/TimerInternals since they model state and timer
 * concepts differently.
 */
public class FnApiDoFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {
  /**
   * A registrar which provides a factory to handle Java {@link DoFn}s.
   */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements
      PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(ParDoTranslation.CUSTOM_JAVA_DO_FN_URN, new Factory());
    }
  }

  /** A factory for {@link FnApiDoFnRunner}. */
  static class Factory<InputT, OutputT>
      implements PTransformRunnerFactory<DoFnRunner<InputT, OutputT>> {

    @Override
    public DoFnRunner<InputT, OutputT> createRunnerForPTransform(
        PipelineOptions pipelineOptions,
        BeamFnDataClient beamFnDataClient,
        BeamFnStateClient beamFnStateClient,
        String pTransformId,
        RunnerApi.PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, RunnerApi.PCollection> pCollections,
        Map<String, RunnerApi.Coder> coders,
        Multimap<String, ThrowingConsumer<WindowedValue<?>>> pCollectionIdsToConsumers,
        Consumer<ThrowingRunnable> addStartFunction,
        Consumer<ThrowingRunnable> addFinishFunction) {

      // For every output PCollection, create a map from output name to Consumer
      ImmutableMap.Builder<String, Collection<ThrowingConsumer<WindowedValue<?>>>>
          outputMapBuilder = ImmutableMap.builder();
      for (Map.Entry<String, String> entry : pTransform.getOutputsMap().entrySet()) {
        outputMapBuilder.put(
            entry.getKey(),
            pCollectionIdsToConsumers.get(entry.getValue()));
      }
      ImmutableMap<String, Collection<ThrowingConsumer<WindowedValue<?>>>> outputMap =
          outputMapBuilder.build();

      // Get the DoFnInfo from the serialized blob.
      ByteString serializedFn = pTransform.getSpec().getPayload();
      @SuppressWarnings({"unchecked", "rawtypes"})
      DoFnInfo<InputT, OutputT> doFnInfo = (DoFnInfo) SerializableUtils.deserializeFromByteArray(
          serializedFn.toByteArray(), "DoFnInfo");

      // Verify that the DoFnInfo tag to output map matches the output map on the PTransform.
      checkArgument(
          Objects.equals(
              new HashSet<>(Collections2.transform(outputMap.keySet(), Long::parseLong)),
              doFnInfo.getOutputMap().keySet()),
          "Unexpected mismatch between transform output map %s and DoFnInfo output map %s.",
          outputMap.keySet(),
          doFnInfo.getOutputMap());

      ImmutableMultimap.Builder<TupleTag<?>,
          ThrowingConsumer<WindowedValue<?>>> tagToOutputMapBuilder =
          ImmutableMultimap.builder();
      for (Map.Entry<Long, TupleTag<?>> entry : doFnInfo.getOutputMap().entrySet()) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Collection<ThrowingConsumer<WindowedValue<?>>> consumers =
            outputMap.get(Long.toString(entry.getKey()));
        tagToOutputMapBuilder.putAll(entry.getValue(), consumers);
      }

      ImmutableMultimap<TupleTag<?>, ThrowingConsumer<WindowedValue<?>>> tagToOutputMap =
          tagToOutputMapBuilder.build();

      // TODO: Swap to using the SideInput definition found on the PTransform once that
      // becomes available within the SDK harness.
      ImmutableMap.Builder<TupleTag<?>, PCollectionView<?>> tagToSideInputMap =
          ImmutableMap.builder();
      for (PCollectionView<?> view : doFnInfo.getSideInputViews()) {
        tagToSideInputMap.put(view.getTagInternal(), view);
      }

      @SuppressWarnings({"unchecked", "rawtypes"})
      DoFnRunner<InputT, OutputT> runner = new FnApiDoFnRunner<>(
          pipelineOptions,
          beamFnStateClient,
          pTransformId,
          processBundleInstructionId,
          doFnInfo.getDoFn(),
          WindowedValue.getFullCoder(
              doFnInfo.getInputCoder(),
              doFnInfo.getWindowingStrategy().getWindowFn().windowCoder()),
          (Collection<ThrowingConsumer<WindowedValue<OutputT>>>) (Collection)
              tagToOutputMap.get(doFnInfo.getOutputMap().get(doFnInfo.getMainOutput())),
          tagToOutputMap,
          tagToSideInputMap.build(),
          doFnInfo.getWindowingStrategy());

      // Register the appropriate handlers.
      addStartFunction.accept(runner::startBundle);
      for (String pcollectionId : pTransform.getInputsMap().values()) {
        pCollectionIdsToConsumers.put(
            pcollectionId,
            (ThrowingConsumer) (ThrowingConsumer<WindowedValue<InputT>>) runner::processElement);
      }
      addFinishFunction.accept(runner::finishBundle);
      return runner;
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////

  private final PipelineOptions pipelineOptions;
  private final BeamFnStateClient beamFnStateClient;
  private final String ptransformId;
  private final Supplier<String> processBundleInstructionId;
  private final DoFn<InputT, OutputT> doFn;
  private final WindowedValueCoder<InputT> inputCoder;
  private final Collection<ThrowingConsumer<WindowedValue<OutputT>>> mainOutputConsumers;
  private final Multimap<TupleTag<?>, ThrowingConsumer<WindowedValue<?>>> outputMap;
  private final Map<TupleTag<?>, PCollectionView<?>> sideInputViewMap;
  private final Map<StateKey, Object> stateKeyObjectCache;
  private final WindowingStrategy windowingStrategy;
  private final DoFnSignature doFnSignature;
  private final DoFnInvoker<InputT, OutputT> doFnInvoker;
  private final StateBinder stateBinder;
  private final StartBundleContext startBundleContext;
  private final ProcessBundleContext processBundleContext;
  private final FinishBundleContext finishBundleContext;
  private final Collection<ThrowingRunnable> stateFinalizers;

  /**
   * The lifetime of this member is only valid during {@link #processElement}
   * and is null otherwise.
   */
  @Nullable
  private WindowedValue<InputT> currentElement;

  /**
   * The lifetime of this member is only valid during {@link #processElement}
   * and is null otherwise.
   */
  @Nullable
  private BoundedWindow currentWindow;

  /**
   * This member should only be accessed indirectly by calling
   * {@link #getBagUserStateKey} and is only valid during {@link #processElement}
   * and is null otherwise.
   */
  @Nullable
  private StateKey.BagUserState cachedPartialBagUserStateKey;

  FnApiDoFnRunner(
      PipelineOptions pipelineOptions,
      BeamFnStateClient beamFnStateClient,
      String ptransformId,
      Supplier<String> processBundleInstructionId,
      DoFn<InputT, OutputT> doFn,
      WindowedValueCoder<InputT> inputCoder,
      Collection<ThrowingConsumer<WindowedValue<OutputT>>> mainOutputConsumers,
      Multimap<TupleTag<?>, ThrowingConsumer<WindowedValue<?>>> outputMap,
      Map<TupleTag<?>, PCollectionView<?>> sideInputViewMap,
      WindowingStrategy windowingStrategy) {
    this.pipelineOptions = pipelineOptions;
    this.beamFnStateClient = beamFnStateClient;
    this.ptransformId = ptransformId;
    this.processBundleInstructionId = processBundleInstructionId;
    this.doFn = doFn;
    this.inputCoder = inputCoder;
    this.mainOutputConsumers = mainOutputConsumers;
    this.outputMap = outputMap;
    this.sideInputViewMap = sideInputViewMap;
    this.stateKeyObjectCache = new HashMap<>();
    this.windowingStrategy = windowingStrategy;
    this.doFnSignature = DoFnSignatures.signatureForDoFn(doFn);
    this.doFnInvoker = DoFnInvokers.invokerFor(doFn);
    this.stateBinder = new BeamFnStateBinder();
    this.startBundleContext = new StartBundleContext();
    this.processBundleContext = new ProcessBundleContext();
    this.finishBundleContext = new FinishBundleContext();
    this.stateFinalizers = new ArrayList<>();
  }

  @Override
  public void startBundle() {
    doFnInvoker.invokeStartBundle(startBundleContext);
  }

  @Override
  public void processElement(WindowedValue<InputT> elem) {
    currentElement = elem;
    try {
      Iterator<BoundedWindow> windowIterator =
          (Iterator<BoundedWindow>) elem.getWindows().iterator();
      while (windowIterator.hasNext()) {
        currentWindow = windowIterator.next();
        doFnInvoker.invokeProcessElement(processBundleContext);
      }
    } finally {
      currentElement = null;
      currentWindow = null;
      cachedPartialBagUserStateKey = null;
    }
  }

  @Override
  public void onTimer(
      String timerId,
      BoundedWindow window,
      Instant timestamp,
      TimeDomain timeDomain) {
    throw new UnsupportedOperationException("TODO: Add support for timers");
  }

  @Override
  public void finishBundle() {
    doFnInvoker.invokeFinishBundle(finishBundleContext);

    // Persist all dirty state cells
    try {
      for (ThrowingRunnable runnable : stateFinalizers) {
        runnable.run();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    // TODO: Support caching state data across bundle boundaries.
    stateKeyObjectCache.clear();
  }

  /**
   * Outputs the given element to the specified set of consumers wrapping any exceptions.
   */
  private <T> void outputTo(
      Collection<ThrowingConsumer<WindowedValue<T>>> consumers,
      WindowedValue<T> output) {
    Iterator<ThrowingConsumer<WindowedValue<T>>> consumerIterator;
    try {
      for (ThrowingConsumer<WindowedValue<T>> consumer : consumers) {
        consumer.accept(output);
      }
    } catch (Throwable t) {
      throw UserCodeException.wrap(t);
    }
  }

  /**
   * Provides arguments for a {@link DoFnInvoker} for {@link DoFn.StartBundle @StartBundle}.
   */
  private class StartBundleContext
      extends DoFn<InputT, OutputT>.StartBundleContext
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
    public BoundedWindow window() {
      throw new UnsupportedOperationException(
          "Cannot access window outside of @ProcessElement and @OnTimer methods.");
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(
        DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access FinishBundleContext outside of @FinishBundle method.");
    }

    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access ProcessContext outside of @ProcessElement method.");
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access OnTimerContext outside of @OnTimer methods.");
    }

    @Override
    public RestrictionTracker<?> restrictionTracker() {
      throw new UnsupportedOperationException(
          "Cannot access RestrictionTracker outside of @ProcessElement method.");
    }

    @Override
    public State state(String stateId) {
      throw new UnsupportedOperationException(
          "Cannot access state outside of @ProcessElement and @OnTimer methods.");
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException(
          "Cannot access timers outside of @ProcessElement and @OnTimer methods.");
    }
  }

  /**
   * Provides arguments for a {@link DoFnInvoker} for {@link DoFn.ProcessElement @ProcessElement}.
   */
  private class ProcessBundleContext
      extends DoFn<InputT, OutputT>.ProcessContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {

    private ProcessBundleContext() {
      doFn.super();
    }

    @Override
    public BoundedWindow window() {
      return currentWindow;
    }

    @Override
    public DoFn.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access StartBundleContext outside of @StartBundle method.");
    }

    @Override
    public DoFn.FinishBundleContext finishBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access FinishBundleContext outside of @FinishBundle method.");
    }

    @Override
    public ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("TODO: Add support for timers");
    }

    @Override
    public RestrictionTracker<?> restrictionTracker() {
      throw new UnsupportedOperationException("TODO: Add support for SplittableDoFn");
    }

    @Override
    public State state(String stateId) {
      StateDeclaration stateDeclaration = doFnSignature.stateDeclarations().get(stateId);
      checkNotNull(stateDeclaration, "No state declaration found for %s", stateId);
      StateSpec<?> spec;
      try {
        spec = (StateSpec<?>) stateDeclaration.field().get(doFn);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      return spec.bind(stateId, stateBinder);
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException("TODO: Add support for timers");
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
    public void output(OutputT output) {
      outputTo(mainOutputConsumers,
          WindowedValue.of(
              output,
              currentElement.getTimestamp(),
              currentWindow,
              currentElement.getPane()));
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      outputTo(mainOutputConsumers,
          WindowedValue.of(
              output,
              timestamp,
              currentWindow,
              currentElement.getPane()));
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      Collection<ThrowingConsumer<WindowedValue<T>>> consumers = (Collection) outputMap.get(tag);
      if (consumers == null) {
        throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
      }
      outputTo(consumers,
          WindowedValue.of(
              output,
              currentElement.getTimestamp(),
              currentWindow,
              currentElement.getPane()));
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      Collection<ThrowingConsumer<WindowedValue<T>>> consumers = (Collection) outputMap.get(tag);
      if (consumers == null) {
        throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
      }
      outputTo(consumers,
          WindowedValue.of(
              output,
              timestamp,
              currentWindow,
              currentElement.getPane()));
    }

    @Override
    public InputT element() {
      return currentElement.getValue();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      return bindSideInputView(view);
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
  }

  /**
   * Provides arguments for a {@link DoFnInvoker} for {@link DoFn.FinishBundle @FinishBundle}.
   */
  private class FinishBundleContext
      extends DoFn<InputT, OutputT>.FinishBundleContext
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
    public BoundedWindow window() {
      throw new UnsupportedOperationException(
          "Cannot access window outside of @ProcessElement and @OnTimer methods.");
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access StartBundleContext outside of @StartBundle method.");
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access ProcessContext outside of @ProcessElement method.");
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access OnTimerContext outside of @OnTimer methods.");
    }

    @Override
    public RestrictionTracker<?> restrictionTracker() {
      throw new UnsupportedOperationException(
          "Cannot access RestrictionTracker outside of @ProcessElement method.");
    }

    @Override
    public State state(String stateId) {
      throw new UnsupportedOperationException(
          "Cannot access state outside of @ProcessElement and @OnTimer methods.");
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException(
          "Cannot access timers outside of @ProcessElement and @OnTimer methods.");
    }

    @Override
    public void output(OutputT output, Instant timestamp, BoundedWindow window) {
      outputTo(mainOutputConsumers,
          WindowedValue.of(output, timestamp, window, PaneInfo.NO_FIRING));
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
      Collection<ThrowingConsumer<WindowedValue<T>>> consumers = (Collection) outputMap.get(tag);
      if (consumers == null) {
        throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
      }
      outputTo(consumers,
          WindowedValue.of(output, timestamp, window, PaneInfo.NO_FIRING));
    }
  }

  /**
   * A {@link StateBinder} that uses the Beam Fn State API to read and write user state.
   *
   * <p>TODO: Add support for {@link #bindMap} and {@link #bindSet}. Note that
   * {@link #bindWatermark} should never be implemented.
   */
  private class BeamFnStateBinder implements StateBinder {
    @Override
    public <T> ValueState<T> bindValue(String id, StateSpec<ValueState<T>> spec, Coder<T> coder) {
      return (ValueState<T>) stateKeyObjectCache.computeIfAbsent(
          getBagUserStateKey(id),
          new Function<StateKey, Object>() {
        @Override
        public Object apply(StateKey key) {
          return new ValueState<T>() {
            private final BagUserState<T> impl = createBagUserState(key, coder);

            @Override
            public void clear() {
              impl.clear();
            }

            @Override
            public void write(T input) {
              impl.clear();
              impl.append(input);
            }

            @Override
            public T read() {
              Iterator<T> value = impl.get().iterator();
              if (value.hasNext()) {
                return value.next();
              } else {
                return null;
              }
            }

            @Override
            public ValueState<T> readLater() {
              // TODO: Support prefetching.
              return this;
            }
          };
        }
      });
    }

    @Override
    public <T> BagState<T> bindBag(String id, StateSpec<BagState<T>> spec, Coder<T> elemCoder) {
      return (BagState<T>) stateKeyObjectCache.computeIfAbsent(
          getBagUserStateKey(id),
          new Function<StateKey, Object>() {
        @Override
        public Object apply(StateKey key) {
          return new BagState<T>() {
            private final BagUserState<T> impl = createBagUserState(key, elemCoder);

            @Override
            public void add(T value) {
              impl.append(value);
            }

            @Override
            public ReadableState<Boolean> isEmpty() {
              return ReadableStates.immediate(!impl.get().iterator().hasNext());
            }

            @Override
            public Iterable<T> read() {
              return impl.get();
            }

            @Override
            public BagState<T> readLater() {
              // TODO: Support prefetching.
              return this;
            }

            @Override
            public void clear() {
              impl.clear();
            }
          };
        }
      });
    }

    @Override
    public <T> SetState<T> bindSet(String id, StateSpec<SetState<T>> spec, Coder<T> elemCoder) {
      throw new UnsupportedOperationException("TODO: Add support for a map state to the Fn API.");
    }

    @Override
    public <KeyT, ValueT> MapState<KeyT, ValueT> bindMap(String id,
        StateSpec<MapState<KeyT, ValueT>> spec, Coder<KeyT> mapKeyCoder,
        Coder<ValueT> mapValueCoder) {
      throw new UnsupportedOperationException("TODO: Add support for a map state to the Fn API.");
    }

    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT> bindCombining(
        String id,
        StateSpec<CombiningState<InputT, AccumT, OutputT>> spec, Coder<AccumT> accumCoder,
        CombineFn<InputT, AccumT, OutputT> combineFn) {
      return (CombiningState<InputT, AccumT, OutputT>) stateKeyObjectCache.computeIfAbsent(
          getBagUserStateKey(id),
          new Function<StateKey, Object>() {
        @Override
        public Object apply(StateKey key) {
          // TODO: Support squashing accumulators depending on whether we know of all
          // remote accumulators and local accumulators or just local accumulators.
          return new CombiningState<InputT, AccumT, OutputT>() {
            private final BagUserState<AccumT> impl = createBagUserState(key, accumCoder);

            @Override
            public AccumT getAccum() {
              Iterator<AccumT> iterator = impl.get().iterator();
              if (iterator.hasNext()) {
                return iterator.next();
              }
              return combineFn.createAccumulator();
            }

            @Override
            public void addAccum(AccumT accum) {
              Iterator<AccumT> iterator = impl.get().iterator();

              // Only merge if there was a prior value
              if (iterator.hasNext()) {
                accum = combineFn.mergeAccumulators(ImmutableList.of(iterator.next(), accum));
                // Since there was a prior value, we need to clear.
                impl.clear();
              }

              impl.append(accum);
            }

            @Override
            public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
              return combineFn.mergeAccumulators(accumulators);
            }

            @Override
            public CombiningState<InputT, AccumT, OutputT> readLater() {
              return this;
            }

            @Override
            public OutputT read() {
              Iterator<AccumT> iterator = impl.get().iterator();
              if (iterator.hasNext()) {
                return combineFn.extractOutput(iterator.next());
              }
              return combineFn.defaultValue();
            }

            @Override
            public void add(InputT value) {
              AccumT newAccumulator = combineFn.addInput(getAccum(), value);
              impl.clear();
              impl.append(newAccumulator);
            }

            @Override
            public ReadableState<Boolean> isEmpty() {
              return ReadableStates.immediate(!impl.get().iterator().hasNext());
            }

            @Override
            public void clear() {
              impl.clear();
            }
          };
        }
      });
    }

    @Override
    public <InputT, AccumT, OutputT> CombiningState<InputT, AccumT, OutputT>
    bindCombiningWithContext(
        String id,
        StateSpec<CombiningState<InputT, AccumT, OutputT>> spec,
        Coder<AccumT> accumCoder,
        CombineFnWithContext<InputT, AccumT, OutputT> combineFn) {
      return (CombiningState<InputT, AccumT, OutputT>) stateKeyObjectCache.computeIfAbsent(
          getBagUserStateKey(id),
          new Function<StateKey, Object>() {
            @Override
            public Object apply(StateKey key) {
              return bindCombining(id, spec, accumCoder, CombineFnUtil.bindContext(combineFn,
                  new StateContext<BoundedWindow>() {
                    @Override
                    public PipelineOptions getPipelineOptions() {
                      return pipelineOptions;
                    }

                    @Override
                    public <T> T sideInput(PCollectionView<T> view) {
                      return processBundleContext.sideInput(view);
                    }

                    @Override
                    public BoundedWindow window() {
                      return currentWindow;
                    }
                  }));
            }
          });
    }

    /**
     * @deprecated The Fn API has no plans to implement WatermarkHoldState as of this writing
     * and is waiting on resolution of BEAM-2535.
     */
    @Override
    @Deprecated
    public WatermarkHoldState bindWatermark(String id, StateSpec<WatermarkHoldState> spec,
        TimestampCombiner timestampCombiner) {
      throw new UnsupportedOperationException("WatermarkHoldState is unsupported by the Fn API.");
    }

    private <T> BagUserState<T> createBagUserState(StateKey key, Coder<T> coder) {
      BagUserState rval = new BagUserState<T>(
          beamFnStateClient,
          key.getBagUserState().getUserStateId(),
          coder,
          () -> StateRequest.newBuilder()
              .setInstructionReference(processBundleInstructionId.get())
              .setStateKey(key)
      );
      stateFinalizers.add(rval::asyncClose);
      return rval;
    }
  }

  /**
   * Memoizes a partially built {@link StateKey} saving on the encoding cost of the key and
   * window across multiple state cells for the lifetime of {@link #processElement}.
   *
   * <p>This should only be called during {@link #processElement}.
   */
  private <K> StateKey getBagUserStateKey(String id) {
    if (cachedPartialBagUserStateKey == null) {
      checkState(currentElement.getValue() instanceof KV,
          "Accessing state in unkeyed context. Current element is not a KV: %s.",
          currentElement);
      checkState(inputCoder.getCoderArguments().get(0) instanceof KvCoder,
          "Accessing state in unkeyed context. No keyed coder found.");

      ByteString.Output encodedKeyOut = ByteString.newOutput();

      Coder<K> keyCoder = ((KvCoder<K, ?>) inputCoder.getValueCoder()).getKeyCoder();
      try {
        keyCoder.encode(((KV<K, ?>) currentElement.getValue()).getKey(), encodedKeyOut);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }

      ByteString.Output encodedWindowOut = ByteString.newOutput();
      try {
        windowingStrategy.getWindowFn().windowCoder().encode(currentWindow, encodedWindowOut);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }

      cachedPartialBagUserStateKey = StateKey.BagUserState.newBuilder()
          .setPtransformId(ptransformId)
          .setKey(encodedKeyOut.toByteString())
          .setWindow(encodedWindowOut.toByteString()).buildPartial();
    }
    return StateKey.newBuilder()
        .setBagUserState(cachedPartialBagUserStateKey.toBuilder().setUserStateId(id).build())
        .build();
  }

  private <T> T bindSideInputView(PCollectionView<T> view) {
    // TODO: Use the ViewFn defined on the SideInput once the PTransform proto is available within
    // the SDK harness instead of defining the mapping using legacy types on the SDK harness side.

    // TODO: Use the coder information from the SideInput definition
    FullWindowedValueCoder<?> windowedValueCoder =
        (FullWindowedValueCoder) ((IterableCoder) view.getCoderInternal()).getElemCoder();
    return (T) stateKeyObjectCache.computeIfAbsent(
        createMultimapSideInputKey(
            view.getTagInternal().getId(),
            (WindowMappingFn) view.getWindowMappingFn(),
            windowedValueCoder.getWindowCoder()),
        new Function<StateKey, Object>() {
          @Override
          public Object apply(StateKey key) {
            // TODO: Implement the other side input views on top of the multimap side input.
            if (view.getViewFn() instanceof PCollectionViews.SingletonViewFn) {
              Iterable<?> valueIterable = createMultimapSideInput(
                  key,
                  VoidCoder.of(),
                  windowedValueCoder.getValueCoder()).get(null);
              if (valueIterable.iterator().hasNext()) {
                return Iterables.getOnlyElement(valueIterable);
              } else {
                return ((PCollectionViews.SingletonViewFn) view.getViewFn()).getDefaultValue();
              }
            } else if (view.getViewFn() instanceof PCollectionViews.IterableViewFn) {
              return createMultimapSideInput(
                  key,
                  VoidCoder.of(),
                  windowedValueCoder.getValueCoder()).get(null);
            }
            throw new UnsupportedOperationException(String.format(
                "TODO: Add support for view %s of type %s, currently only %s are supported.",
                view.getTagInternal().getId(),
                view.getViewFn().getClass(),
                ImmutableList.of(PCollectionViews.SingletonViewFn.class,
                    PCollectionViews.IterableViewFn.class)));
          }
        });
  }

  private <K, V> MultimapSideInput<K, V> createMultimapSideInput(
      StateKey stateKey,
      Coder<K> keyCoder,
      Coder<V> valueCoder) {
    return new MultimapSideInput<>(
        beamFnStateClient,
        stateKey.getMultimapSideInput().getSideInputId(),
        keyCoder,
        valueCoder,
        () -> StateRequest.newBuilder()
            .setInstructionReference(processBundleInstructionId.get())
            .setStateKey(stateKey));
  }

  /**
   * TODO: Cache the StateKey on a per PCollectionView and window to save on encoding costs of
   * the window.
   *
   * <p>This should only be called during {@link #processElement}.
   */
  private <W extends BoundedWindow> StateKey createMultimapSideInputKey(
      String id,
      WindowMappingFn<W> windowMappingFn,
      Coder<W> windowCoder) {
    ByteString.Output encodedWindowOut = ByteString.newOutput();
    try {
      windowCoder.encode(windowMappingFn.getSideInputWindow(currentWindow), encodedWindowOut);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    return StateKey.newBuilder()
        .setMultimapSideInput(
            StateKey.MultimapSideInput.newBuilder()
                .setPtransformId(ptransformId)
                .setWindow(encodedWindowOut.toByteString())
                .setSideInputId(id))
        .build();
  }
}
