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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static org.apache.flink.util.Preconditions.checkArgument;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Iterables;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetNewDoFn;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateNamespaces.WindowNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.metrics.DoFnRunnerWithMetricsUpdate;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkBroadcastStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkKeyGroupStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkSplitStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.KeyGroupCheckpointedOperator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.HeapInternalTimerService;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.util.OutputTag;
import org.joda.time.Instant;

/**
 * Flink operator for executing {@link DoFn DoFns}.
 *
 * @param <InputT> the input type of the {@link DoFn}
 * @param <OutputT> the output type of the {@link DoFn}
 */
public class DoFnOperator<InputT, OutputT>
    extends AbstractStreamOperator<WindowedValue<OutputT>>
    implements OneInputStreamOperator<WindowedValue<InputT>, WindowedValue<OutputT>>,
      TwoInputStreamOperator<WindowedValue<InputT>, RawUnionValue, WindowedValue<OutputT>>,
    KeyGroupCheckpointedOperator, Triggerable<Object, TimerData> {

  protected DoFn<InputT, OutputT> doFn;

  protected final SerializedPipelineOptions serializedOptions;

  protected final TupleTag<OutputT> mainOutputTag;
  protected final List<TupleTag<?>> additionalOutputTags;

  protected final Collection<PCollectionView<?>> sideInputs;
  protected final Map<Integer, PCollectionView<?>> sideInputTagMapping;

  protected final WindowingStrategy<?, ?> windowingStrategy;

  protected final OutputManagerFactory<OutputT> outputManagerFactory;

  protected transient DoFnRunner<InputT, OutputT> doFnRunner;
  protected transient PushbackSideInputDoFnRunner<InputT, OutputT> pushbackDoFnRunner;

  protected transient SideInputHandler sideInputHandler;

  protected transient SideInputReader sideInputReader;

  protected transient BufferedOutputManager<OutputT> outputManager;

  private transient DoFnInvoker<InputT, OutputT> doFnInvoker;

  protected transient long currentInputWatermark;

  protected transient long currentSideInputWatermark;

  protected transient long currentOutputWatermark;

  private transient StateTag<BagState<WindowedValue<InputT>>> pushedBackTag;

  protected transient FlinkStateInternals<?> keyedStateInternals;

  private final String stepName;

  private final Coder<WindowedValue<InputT>> inputCoder;

  private final Coder<?> keyCoder;

  private final TimerInternals.TimerDataCoder timerCoder;

  protected transient HeapInternalTimerService<?, TimerInternals.TimerData> timerService;

  protected transient FlinkTimerInternals timerInternals;

  private transient StateInternals nonKeyedStateInternals;

  private transient Optional<Long> pushedBackWatermark;

  // bundle control
  private transient boolean bundleStarted = false;
  private transient Long maxBundleSize;
  private transient Long maxBundleTime;
  private transient Long bundleCount;
  private transient Long lastFinishBundleTime;
  private transient ScheduledFuture<?> checkFinishBundleTimer;

  public DoFnOperator(
      DoFn<InputT, OutputT> doFn,
      String stepName,
      Coder<WindowedValue<InputT>> inputCoder,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      OutputManagerFactory<OutputT> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options,
      Coder<?> keyCoder) {
    this.doFn = doFn;
    this.stepName = stepName;
    this.inputCoder = inputCoder;
    this.mainOutputTag = mainOutputTag;
    this.additionalOutputTags = additionalOutputTags;
    this.sideInputTagMapping = sideInputTagMapping;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializedPipelineOptions(options);
    this.windowingStrategy = windowingStrategy;
    this.outputManagerFactory = outputManagerFactory;

    setChainingStrategy(ChainingStrategy.ALWAYS);

    this.keyCoder = keyCoder;

    this.timerCoder =
        TimerInternals.TimerDataCoder.of(windowingStrategy.getWindowFn().windowCoder());
  }

  // allow overriding this in WindowDoFnOperator because this one dynamically creates
  // the DoFn
  protected DoFn<InputT, OutputT> getDoFn() {
    return doFn;
  }

  @Override
  public void open() throws Exception {
    super.open();

    setCurrentInputWatermark(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis());
    setCurrentSideInputWatermark(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis());
    setCurrentOutputWatermark(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis());

    FlinkPipelineOptions options =
        serializedOptions.getPipelineOptions().as(FlinkPipelineOptions.class);
    sideInputReader = NullSideInputReader.of(sideInputs);

    // maybe init by initializeState
    if (nonKeyedStateInternals == null) {
      if (keyCoder != null) {
        nonKeyedStateInternals = new FlinkKeyGroupStateInternals<>(keyCoder,
            getKeyedStateBackend());
      } else {
        nonKeyedStateInternals =
            new FlinkSplitStateInternals<>(getOperatorStateBackend());
      }
    }

    if (!sideInputs.isEmpty()) {

      pushedBackTag = StateTags.bag("pushed-back-values", inputCoder);

      FlinkBroadcastStateInternals sideInputStateInternals =
          new FlinkBroadcastStateInternals<>(
              getContainingTask().getIndexInSubtaskGroup(), getOperatorStateBackend());

      sideInputHandler = new SideInputHandler(sideInputs, sideInputStateInternals);
      sideInputReader = sideInputHandler;

      pushedBackWatermark = Optional.absent();
    }

    outputManager = outputManagerFactory.create(output, nonKeyedStateInternals);

    // StatefulPardo or WindowDoFn
    if (keyCoder != null) {
      keyedStateInternals = new FlinkStateInternals<>((KeyedStateBackend) getKeyedStateBackend(),
          keyCoder);

      timerService = (HeapInternalTimerService<?, TimerInternals.TimerData>)
          getInternalTimerService("beam-timer", new CoderTypeSerializer<>(timerCoder), this);

      timerInternals = new FlinkTimerInternals();

    }

    // WindowDoFnOperator need use state and timer to get DoFn.
    // So must wait StateInternals and TimerInternals ready.
    this.doFn = getDoFn();
    doFnInvoker = DoFnInvokers.invokerFor(doFn);

    doFnInvoker.invokeSetup();

    StepContext stepContext = new FlinkStepContext();

    doFnRunner = DoFnRunners.simpleRunner(
        options,
        doFn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        additionalOutputTags,
        stepContext,
        windowingStrategy);

    if (doFn instanceof GroupAlsoByWindowViaWindowSetNewDoFn) {
      // When the doFn is this, we know it came from WindowDoFnOperator and
      //   InputT = KeyedWorkItem<K, V>
      //   OutputT = KV<K, V>
      //
      // for some K, V


      doFnRunner = DoFnRunners.lateDataDroppingRunner(
          (DoFnRunner) doFnRunner,
          stepContext,
          windowingStrategy);
    } else if (keyCoder != null) {
      // It is a stateful DoFn

      StatefulDoFnRunner.CleanupTimer cleanupTimer =
          new StatefulDoFnRunner.TimeInternalsCleanupTimer(
              stepContext.timerInternals(), windowingStrategy);

      // we don't know the window type
      @SuppressWarnings({"unchecked", "rawtypes"})
      Coder windowCoder = windowingStrategy.getWindowFn().windowCoder();

      @SuppressWarnings({"unchecked", "rawtypes"})
      StatefulDoFnRunner.StateCleaner<?> stateCleaner =
          new StatefulDoFnRunner.StateInternalsStateCleaner<>(
              doFn, stepContext.stateInternals(), windowCoder);

      doFnRunner = DoFnRunners.defaultStatefulDoFnRunner(
          doFn,
          doFnRunner,
          windowingStrategy,
          cleanupTimer,
          stateCleaner);
    }

    if (options.getEnableMetrics()) {
      doFnRunner = new DoFnRunnerWithMetricsUpdate<>(stepName, doFnRunner, getRuntimeContext());
    }

    maxBundleSize = options.getMaxBundleSize();
    maxBundleTime = options.getMaxBundleTime();
    bundleCount = 0L;
    lastFinishBundleTime = getProcessingTimeService().getCurrentProcessingTime();
    // Schedule timer to check timeout of finish bundle.
    // Period is maxBundleTime/2 to ensure that there is at least once a finishBundle
    // within maxBundleTime.
    checkFinishBundleTimer = getProcessingTimeService().scheduleAtFixedRate(
        new ProcessingTimeCallback() {
          @Override
          public void onProcessingTime(long timestamp) throws Exception {
            checkInvokeFinishBundleByTime();
          }
        },
        maxBundleTime / 2,
        maxBundleTime / 2
    );

    pushbackDoFnRunner =
        SimplePushbackSideInputDoFnRunner.create(doFnRunner, sideInputs, sideInputHandler);
  }

  @Override
  public void close() throws Exception {
    super.close();

    // sanity check: these should have been flushed out by +Inf watermarks
    if (!sideInputs.isEmpty() && nonKeyedStateInternals != null) {
      BagState<WindowedValue<InputT>> pushedBack =
          nonKeyedStateInternals.state(StateNamespaces.global(), pushedBackTag);

      Iterable<WindowedValue<InputT>> pushedBackContents = pushedBack.read();
      if (pushedBackContents != null) {
        if (!Iterables.isEmpty(pushedBackContents)) {
          String pushedBackString = Joiner.on(",").join(pushedBackContents);
          throw new RuntimeException(
              "Leftover pushed-back data: " + pushedBackString + ". This indicates a bug.");
        }
      }
    }
    checkFinishBundleTimer.cancel(true);
    doFnInvoker.invokeTeardown();
  }

  private long getPushbackWatermarkHold() {
    // if we don't have side inputs we never hold the watermark
    if (sideInputs.isEmpty()) {
      return BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
    }

    try {
      checkInitPushedBackWatermark();
      return pushedBackWatermark.get();
    } catch (Exception e) {
      throw new RuntimeException("Error retrieving pushed back watermark state.", e);
    }
  }

  private void checkInitPushedBackWatermark() {
    // init and restore from pushedBack state.
    // Not done in initializeState, because OperatorState is not ready.
    if (!pushedBackWatermark.isPresent()) {

      BagState<WindowedValue<InputT>> pushedBack =
          nonKeyedStateInternals.state(StateNamespaces.global(), pushedBackTag);

      long min = BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
      for (WindowedValue<InputT> value : pushedBack.read()) {
        min = Math.min(min, value.getTimestamp().getMillis());
      }
      setPushedBackWatermark(min);
    }
  }

  @Override
  public final void processElement(
      StreamRecord<WindowedValue<InputT>> streamRecord) throws Exception {
    checkInvokeStartBundle();
    doFnRunner.processElement(streamRecord.getValue());
    checkInvokeFinishBundleByCount();
  }

  private void setPushedBackWatermark(long watermark) {
    pushedBackWatermark = Optional.fromNullable(watermark);
  }

  @Override
  public final void processElement1(
      StreamRecord<WindowedValue<InputT>> streamRecord) throws Exception {
    checkInvokeStartBundle();
    Iterable<WindowedValue<InputT>> justPushedBack =
        pushbackDoFnRunner.processElementInReadyWindows(streamRecord.getValue());

    BagState<WindowedValue<InputT>> pushedBack =
        nonKeyedStateInternals.state(StateNamespaces.global(), pushedBackTag);

    checkInitPushedBackWatermark();

    long min = pushedBackWatermark.get();
    for (WindowedValue<InputT> pushedBackValue : justPushedBack) {
      min = Math.min(min, pushedBackValue.getTimestamp().getMillis());
      pushedBack.add(pushedBackValue);
    }
    setPushedBackWatermark(min);
    checkInvokeFinishBundleByCount();
  }

  @Override
  public final void processElement2(
      StreamRecord<RawUnionValue> streamRecord) throws Exception {
    checkInvokeStartBundle();

    @SuppressWarnings("unchecked")
    WindowedValue<Iterable<?>> value =
        (WindowedValue<Iterable<?>>) streamRecord.getValue().getValue();

    PCollectionView<?> sideInput = sideInputTagMapping.get(streamRecord.getValue().getUnionTag());
    sideInputHandler.addSideInputValue(sideInput, value);

    BagState<WindowedValue<InputT>> pushedBack =
        nonKeyedStateInternals.state(StateNamespaces.global(), pushedBackTag);

    List<WindowedValue<InputT>> newPushedBack = new ArrayList<>();

    Iterable<WindowedValue<InputT>> pushedBackContents = pushedBack.read();
    if (pushedBackContents != null) {
      for (WindowedValue<InputT> elem : pushedBackContents) {

        // we need to set the correct key in case the operator is
        // a (keyed) window operator
        setKeyContextElement1(new StreamRecord<>(elem));

        Iterable<WindowedValue<InputT>> justPushedBack =
            pushbackDoFnRunner.processElementInReadyWindows(elem);
        Iterables.addAll(newPushedBack, justPushedBack);
      }
    }

    pushedBack.clear();
    long min = BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
    for (WindowedValue<InputT> pushedBackValue : newPushedBack) {
      min = Math.min(min, pushedBackValue.getTimestamp().getMillis());
      pushedBack.add(pushedBackValue);
    }
    setPushedBackWatermark(min);

    checkInvokeFinishBundleByCount();

    // maybe output a new watermark
    processWatermark1(new Watermark(currentInputWatermark));
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    processWatermark1(mark);
  }

  @Override
  public void processWatermark1(Watermark mark) throws Exception {

    checkInvokeStartBundle();

    // We do the check here because we are guaranteed to at least get the +Inf watermark on the
    // main input when the job finishes.
    if (currentSideInputWatermark >= BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
      // this means we will never see any more side input
      // we also do the check here because we might have received the side-input MAX watermark
      // before receiving any main-input data
      emitAllPushedBackData();
    }

    if (keyCoder == null) {
      setCurrentInputWatermark(mark.getTimestamp());
      long potentialOutputWatermark =
          Math.min(getPushbackWatermarkHold(), currentInputWatermark);
      if (potentialOutputWatermark > currentOutputWatermark) {
        setCurrentOutputWatermark(potentialOutputWatermark);
        emitWatermark(currentOutputWatermark);
      }
    } else {
      setCurrentInputWatermark(mark.getTimestamp());

      // hold back by the pushed back values waiting for side inputs
      long actualInputWatermark = Math.min(getPushbackWatermarkHold(), mark.getTimestamp());

      timerService.advanceWatermark(actualInputWatermark);

      Instant watermarkHold = keyedStateInternals.watermarkHold();

      long combinedWatermarkHold = Math.min(watermarkHold.getMillis(), getPushbackWatermarkHold());

      long potentialOutputWatermark = Math.min(currentInputWatermark, combinedWatermarkHold);

      if (potentialOutputWatermark > currentOutputWatermark) {
        setCurrentOutputWatermark(potentialOutputWatermark);
        emitWatermark(currentOutputWatermark);
      }
    }
  }

  private void emitWatermark(long watermark) {
    // Must invoke finishBatch before emit the +Inf watermark otherwise there are some late events.
    if (watermark >= BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
      invokeFinishBundle();
    }
    output.emitWatermark(new Watermark(watermark));
  }

  @Override
  public void processWatermark2(Watermark mark) throws Exception {
    checkInvokeStartBundle();

    setCurrentSideInputWatermark(mark.getTimestamp());
    if (mark.getTimestamp() >= BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
      // this means we will never see any more side input
      emitAllPushedBackData();

      // maybe output a new watermark
      processWatermark1(new Watermark(currentInputWatermark));
    }

  }

  /**
   * Emits all pushed-back data. This should be used once we know that there will not be
   * any future side input, i.e. that there is no point in waiting.
   */
  private void emitAllPushedBackData() throws Exception {

    BagState<WindowedValue<InputT>> pushedBack =
        nonKeyedStateInternals.state(StateNamespaces.global(), pushedBackTag);

    Iterable<WindowedValue<InputT>> pushedBackContents = pushedBack.read();
    if (pushedBackContents != null) {
      for (WindowedValue<InputT> elem : pushedBackContents) {

        // we need to set the correct key in case the operator is
        // a (keyed) window operator
        setKeyContextElement1(new StreamRecord<>(elem));

        doFnRunner.processElement(elem);
      }
    }

    pushedBack.clear();

    setPushedBackWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());

  }

  /**
   * Check whether invoke startBundle, if it is, need to output elements that were
   * buffered as part of finishing a bundle in snapshot() first.
   *
   * <p>In order to avoid having {@link DoFnRunner#processElement(WindowedValue)} or
   * {@link DoFnRunner#onTimer(String, BoundedWindow, Instant, TimeDomain)} not between
   * StartBundle and FinishBundle, this method needs to be called in each processElement
   * and each processWatermark and onProcessingTime. Do not need to call in onEventTime,
   * because it has been guaranteed in the processWatermark.
   */
  private void checkInvokeStartBundle() {
    if (!bundleStarted) {
      outputManager.flushBuffer();
      pushbackDoFnRunner.startBundle();
      bundleStarted = true;
    }
  }

  /**
   * Check whether invoke finishBundle by elements count. Called in processElement.
   */
  private void checkInvokeFinishBundleByCount() {
    bundleCount++;
    if (bundleCount >= maxBundleSize) {
      invokeFinishBundle();
    }
  }

  /**
   * Check whether invoke finishBundle by timeout.
   */
  private void checkInvokeFinishBundleByTime() {
    long now = getProcessingTimeService().getCurrentProcessingTime();
    if (now - lastFinishBundleTime >= maxBundleTime) {
      invokeFinishBundle();
    }
  }

  private void invokeFinishBundle() {
    if (bundleStarted) {
      pushbackDoFnRunner.finishBundle();
      bundleStarted = false;
      bundleCount = 0L;
      lastFinishBundleTime = getProcessingTimeService().getCurrentProcessingTime();
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {

    // Forced finish a bundle in checkpoint barrier otherwise may lose data.
    // Careful, it use OperatorState or KeyGroupState to store outputs, So it
    // must be called before their snapshot.
    outputManager.openBuffer();
    invokeFinishBundle();
    outputManager.closeBuffer();

    // copy from AbstractStreamOperator
    if (getKeyedStateBackend() != null) {
      KeyedStateCheckpointOutputStream out;

      try {
        out = context.getRawKeyedOperatorStateOutput();
      } catch (Exception exception) {
        throw new Exception("Could not open raw keyed operator state stream for "
            + getOperatorName() + '.', exception);
      }

      try {
        KeyGroupsList allKeyGroups = out.getKeyGroupList();
        for (int keyGroupIdx : allKeyGroups) {
          out.startNewKeyGroup(keyGroupIdx);

          DataOutputViewStreamWrapper dov = new DataOutputViewStreamWrapper(out);

          // if (this instanceof KeyGroupCheckpointedOperator)
          snapshotKeyGroupState(keyGroupIdx, dov);

          // We can't get all timerServices, so we just snapshot our timerService
          // Maybe this is a normal DoFn that has no timerService
          if (keyCoder != null) {
            timerService.snapshotTimersForKeyGroup(dov, keyGroupIdx);
          }

        }
      } catch (Exception exception) {
        throw new Exception("Could not write timer service of " + getOperatorName()
            + " to checkpoint state stream.", exception);
      } finally {
        try {
          out.close();
        } catch (Exception closeException) {
          LOG.warn("Could not close raw keyed operator state stream for {}. This "
              + "might have prevented deleting some state data.", getOperatorName(),
              closeException);
        }
      }
    }
  }

  @Override
  public void snapshotKeyGroupState(int keyGroupIndex, DataOutputStream out) throws Exception {
    if (keyCoder != null) {
      ((FlinkKeyGroupStateInternals) nonKeyedStateInternals).snapshotKeyGroupState(
          keyGroupIndex, out);
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    if (getKeyedStateBackend() != null) {
      int totalKeyGroups = getKeyedStateBackend().getNumberOfKeyGroups();
      KeyGroupsList localKeyGroupRange = getKeyedStateBackend().getKeyGroupRange();

      for (KeyGroupStatePartitionStreamProvider streamProvider : context.getRawKeyedStateInputs()) {
        DataInputViewStreamWrapper div = new DataInputViewStreamWrapper(streamProvider.getStream());

        int keyGroupIdx = streamProvider.getKeyGroupId();
        checkArgument(localKeyGroupRange.contains(keyGroupIdx),
            "Key Group " + keyGroupIdx + " does not belong to the local range.");

        // if (this instanceof KeyGroupRestoringOperator)
        restoreKeyGroupState(keyGroupIdx, div);

        // We just initialize our timerService
        if (keyCoder != null) {
          if (timerService == null) {
            timerService = new HeapInternalTimerService<>(
                totalKeyGroups,
                localKeyGroupRange,
                this,
                getRuntimeContext().getProcessingTimeService());
          }
          timerService.restoreTimersForKeyGroup(div, keyGroupIdx, getUserCodeClassloader());
        }
      }
    }
  }

  @Override
  public void restoreKeyGroupState(int keyGroupIndex, DataInputStream in) throws Exception {
    if (keyCoder != null) {
      if (nonKeyedStateInternals == null) {
        nonKeyedStateInternals = new FlinkKeyGroupStateInternals<>(keyCoder,
            getKeyedStateBackend());
      }
      ((FlinkKeyGroupStateInternals) nonKeyedStateInternals)
          .restoreKeyGroupState(keyGroupIndex, in, getUserCodeClassloader());
    }
  }

  @Override
  public void onEventTime(InternalTimer<Object, TimerData> timer) throws Exception {
    fireTimer(timer);
  }

  @Override
  public void onProcessingTime(InternalTimer<Object, TimerData> timer) throws Exception {
    checkInvokeStartBundle();
    fireTimer(timer);
  }

  // allow overriding this in WindowDoFnOperator
  public void fireTimer(InternalTimer<?, TimerData> timer) {
    TimerInternals.TimerData timerData = timer.getNamespace();
    StateNamespace namespace = timerData.getNamespace();
    // This is a user timer, so namespace must be WindowNamespace
    checkArgument(namespace instanceof WindowNamespace);
    BoundedWindow window = ((WindowNamespace) namespace).getWindow();
    pushbackDoFnRunner.onTimer(timerData.getTimerId(), window,
        timerData.getTimestamp(), timerData.getDomain());
  }

  private void setCurrentInputWatermark(long currentInputWatermark) {
    this.currentInputWatermark = currentInputWatermark;
  }

  private void setCurrentSideInputWatermark(long currentInputWatermark) {
    this.currentSideInputWatermark = currentInputWatermark;
  }

  private void setCurrentOutputWatermark(long currentOutputWatermark) {
    this.currentOutputWatermark = currentOutputWatermark;
  }

  /**
   * Factory for creating an {@link BufferedOutputManager} from
   * a Flink {@link Output}.
   */
  interface OutputManagerFactory<OutputT> extends Serializable {
    BufferedOutputManager<OutputT> create(
        Output<StreamRecord<WindowedValue<OutputT>>> output,
        StateInternals stateInternals);
  }

  /**
   * A {@link DoFnRunners.OutputManager} that can buffer its outputs.
   * Use {@link FlinkSplitStateInternals} or {@link FlinkKeyGroupStateInternals}
   * to keep buffer data.
   */
  public static class BufferedOutputManager<OutputT> implements
      DoFnRunners.OutputManager {

    private final BiMap<TupleTag<?>, Integer> tagToLabel;
    private TupleTag<OutputT> mainTag;
    private Map<TupleTag<?>, OutputTag<WindowedValue<?>>> mapping;
    protected Output<StreamRecord<WindowedValue<OutputT>>> output;

    private boolean openBuffer = false;
    private BagState<KV<Integer, WindowedValue<?>>> bufferState;

    BufferedOutputManager(
        Output<StreamRecord<WindowedValue<OutputT>>> output,
        TupleTag<OutputT> mainTag,
        Map<TupleTag<?>, OutputTag<WindowedValue<?>>> mapping,
        final Map<TupleTag<?>, Coder<WindowedValue<?>>> coderMapping,
        StateInternals stateInternals) {
      this.output = output;
      this.mainTag = mainTag;
      this.mapping = mapping;

      tagToLabel = transformTupleTagsToLabels(mainTag, mapping);

      StateTag<BagState<KV<Integer, WindowedValue<?>>>> bufferTag =
          StateTags.bag("bundle-buffer-tag", new TaggedKvCoder(tagToLabel, coderMapping));
      bufferState = stateInternals.state(StateNamespaces.global(), bufferTag);
    }

    /**
     * Transform TupleTags to labels. Integer is easier to serialize than TupleTag.
     * Returns a BiMap for easier reversal query.
     */
    private static BiMap<TupleTag<?>, Integer> transformTupleTagsToLabels(
        TupleTag<?> mainTag,
        Map<TupleTag<?>, OutputTag<WindowedValue<?>>> mapping) {

      BiMap<TupleTag<?>, Integer> tagToLabel = HashBiMap.create();
      int count = 0;
      tagToLabel.put(mainTag, count++);
      for (TupleTag<?> key : mapping.keySet()) {
        if (!tagToLabel.containsKey(key)) {
          tagToLabel.put(key, count++);
        }
      }
      return tagToLabel;
    }

    void openBuffer() {
      this.openBuffer = true;
    }

    void closeBuffer() {
      this.openBuffer = false;
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> value) {
      if (!openBuffer) {
        emit(tag, value);
      } else {
        bufferState.add(KV.<Integer, WindowedValue<?>>of(tagToLabel.get(tag), value));
      }
    }

    /**
     * Flush elements of bufferState to Flink Output. This method can't be invoke in
     * {@link #snapshotState(StateSnapshotContext)}
     */
    void flushBuffer() {
      for (KV<Integer, WindowedValue<?>> tuple2 : bufferState.read()) {
        emit(tagToLabel.inverse().get(tuple2.getKey()), (WindowedValue) tuple2.getValue());
      }
      bufferState.clear();
    }

    private <T> void emit(TupleTag<T> tag, WindowedValue<T> value) {
      if (tag.equals(mainTag)) {
        // with tagged outputs we can't get around this because we don't
        // know our own output type...
        @SuppressWarnings("unchecked")
        WindowedValue<OutputT> castValue = (WindowedValue<OutputT>) value;
        output.collect(new StreamRecord<>(castValue));
      } else {
        @SuppressWarnings("unchecked")
        OutputTag<WindowedValue<T>> outputTag = (OutputTag) mapping.get(tag);
        output.collect(outputTag, new StreamRecord<>(value));
      }
    }
  }

  /**
   * Coder for KV of label and value. It will be serialized in Flink checkpoint.
   */
  private static class TaggedKvCoder extends StructuredCoder<KV<Integer, WindowedValue<?>>> {

    private BiMap<TupleTag<?>, Integer> tagToLabel;
    private Map<TupleTag<?>, Coder<WindowedValue<?>>> coderMapping;

    TaggedKvCoder(
        BiMap<TupleTag<?>, Integer> tagToLabel,
        Map<TupleTag<?>, Coder<WindowedValue<?>>> coderMapping) {
      this.tagToLabel = tagToLabel;
      this.coderMapping = coderMapping;
    }

    @Override
    public void encode(KV<Integer, WindowedValue<?>> kv, OutputStream out)
        throws IOException {
      Coder<WindowedValue<?>> coder = coderMapping.get(tagToLabel.inverse().get(kv.getKey()));
      VarIntCoder.of().encode(kv.getKey(), out);
      coder.encode(kv.getValue(), out);
    }

    @Override
    public KV<Integer, WindowedValue<?>> decode(InputStream in)
        throws IOException {
      Integer label = VarIntCoder.of().decode(in);
      Coder<WindowedValue<?>> coder = coderMapping.get(tagToLabel.inverse().get(label));
      WindowedValue<?> value = coder.decode(in);
      return KV.<Integer, WindowedValue<?>>of(label, value);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}
  }

  /**
   * Implementation of {@link OutputManagerFactory} that creates an
   * {@link BufferedOutputManager} that can write to multiple logical
   * outputs by Flink side output.
   */
  public static class MultiOutputOutputManagerFactory<OutputT>
      implements OutputManagerFactory<OutputT> {

    private TupleTag<OutputT> mainTag;
    private Map<TupleTag<?>, OutputTag<WindowedValue<?>>> mapping;
    private Map<TupleTag<?>, Coder<WindowedValue<?>>> coderMapping;

    public MultiOutputOutputManagerFactory(
        TupleTag<OutputT> mainTag) {
      this(mainTag,
          new HashMap<TupleTag<?>, OutputTag<WindowedValue<?>>>(),
          new HashMap<TupleTag<?>, Coder<WindowedValue<?>>>());
    }

    public MultiOutputOutputManagerFactory(
        TupleTag<OutputT> mainTag,
        Map<TupleTag<?>, OutputTag<WindowedValue<?>>> mapping,
        Map<TupleTag<?>, Coder<WindowedValue<?>>> coderMapping) {
      this.mainTag = mainTag;
      this.mapping = mapping;
      this.coderMapping = coderMapping;
    }

    @Override
    public BufferedOutputManager<OutputT> create(
        Output<StreamRecord<WindowedValue<OutputT>>> output,
        StateInternals stateInternals) {
      return new BufferedOutputManager<>(output, mainTag, mapping, coderMapping, stateInternals);
    }
  }

  /**
   * {@link StepContext} for running {@link DoFn DoFns} on Flink. This does not allow
   * accessing state or timer internals.
   */
  protected class FlinkStepContext implements StepContext {

    @Override
    public StateInternals stateInternals() {
      return keyedStateInternals;
    }

    @Override
    public TimerInternals timerInternals() {
      return timerInternals;
    }
  }

  private class FlinkTimerInternals implements TimerInternals {

    @Override
    public void setTimer(
        StateNamespace namespace, String timerId, Instant target, TimeDomain timeDomain) {
      setTimer(TimerData.of(timerId, namespace, target, timeDomain));
    }

    @Deprecated
    @Override
    public void setTimer(TimerData timerKey) {
      long time = timerKey.getTimestamp().getMillis();
      switch (timerKey.getDomain()) {
        case EVENT_TIME:
          timerService.registerEventTimeTimer(timerKey, time);
          break;
        case PROCESSING_TIME:
        case SYNCHRONIZED_PROCESSING_TIME:
          timerService.registerProcessingTimeTimer(timerKey, time);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported time domain: " + timerKey.getDomain());
      }
    }

    @Deprecated
    @Override
    public void deleteTimer(StateNamespace namespace, String timerId) {
      throw new UnsupportedOperationException(
          "Canceling of a timer by ID is not yet supported.");
    }

    @Override
    public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
      throw new UnsupportedOperationException(
          "Canceling of a timer by ID is not yet supported.");
    }

    @Deprecated
    @Override
    public void deleteTimer(TimerData timerKey) {
      long time = timerKey.getTimestamp().getMillis();
      switch (timerKey.getDomain()) {
        case EVENT_TIME:
          timerService.deleteEventTimeTimer(timerKey, time);
          break;
        case PROCESSING_TIME:
        case SYNCHRONIZED_PROCESSING_TIME:
          timerService.deleteProcessingTimeTimer(timerKey, time);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported time domain: " + timerKey.getDomain());
      }
    }

    @Override
    public Instant currentProcessingTime() {
      return new Instant(timerService.currentProcessingTime());
    }

    @Nullable
    @Override
    public Instant currentSynchronizedProcessingTime() {
      return new Instant(timerService.currentProcessingTime());
    }

    @Override
    public Instant currentInputWatermarkTime() {
      return new Instant(Math.min(currentInputWatermark, getPushbackWatermarkHold()));
    }

    @Nullable
    @Override
    public Instant currentOutputWatermarkTime() {
      return new Instant(currentOutputWatermark);
    }
  }
}
