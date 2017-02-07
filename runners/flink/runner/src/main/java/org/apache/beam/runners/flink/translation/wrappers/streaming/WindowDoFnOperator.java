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

import static org.apache.beam.runners.core.TimerInternals.TimerData;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.ExecutionContext;
import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetNewDoFn;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.HeapInternalTimerService;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.joda.time.Instant;

/**
 * Flink operator for executing window {@link DoFn DoFns}.
 *
 * @param <InputT>
 * @param <OutputT>
 */
public class WindowDoFnOperator<K, InputT, OutputT>
    extends DoFnOperator<KeyedWorkItem<K, InputT>, KV<K, OutputT>, WindowedValue<KV<K, OutputT>>>
    implements Triggerable<K, TimerData> {

  private final TimerInternals.TimerDataCoder timerCoder;

  private transient FlinkTimerInternals timerInternals;

  private final SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> systemReduceFn;

  private transient HeapInternalTimerService<K, TimerData> timerService;

  public WindowDoFnOperator(
      SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> systemReduceFn,
      TypeInformation<WindowedValue<KeyedWorkItem<K, InputT>>> inputType,
      TupleTag<KV<K, OutputT>> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      OutputManagerFactory<WindowedValue<KV<K, OutputT>>> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options,
      Coder<K> keyCoder) {
    super(
        null,
        inputType,
        mainOutputTag,
        sideOutputTags,
        outputManagerFactory,
        windowingStrategy,
        sideInputTagMapping,
        sideInputs,
        options,
        keyCoder);

    this.systemReduceFn = systemReduceFn;

    this.timerCoder =
        TimerInternals.TimerDataCoder.of(windowingStrategy.getWindowFn().windowCoder());
  }

  @Override
  protected DoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> getDoFn() {
    StateInternalsFactory<K> stateInternalsFactory = new StateInternalsFactory<K>() {
      @Override
      public StateInternals<K> stateInternalsForKey(K key) {
        //this will implicitly be keyed by the key of the incoming
        // element or by the key of a firing timer
        return (StateInternals<K>) stateInternals;
      }
    };
    TimerInternalsFactory<K> timerInternalsFactory = new TimerInternalsFactory<K>() {
      @Override
      public TimerInternals timerInternalsForKey(K key) {
        //this will implicitly be keyed like the StateInternalsFactory
        return timerInternals;
      }
    };

    // we have to do the unchecked cast because GroupAlsoByWindowViaWindowSetDoFn.create
    // has the window type as generic parameter while WindowingStrategy is almost always
    // untyped.
    @SuppressWarnings("unchecked")
    DoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> doFn =
        GroupAlsoByWindowViaWindowSetNewDoFn.create(
            windowingStrategy, stateInternalsFactory, timerInternalsFactory, sideInputReader,
                (SystemReduceFn) systemReduceFn, outputManager, mainOutputTag);
    return doFn;
  }


  @Override
  public void open() throws Exception {

    timerService = (HeapInternalTimerService<K, TimerData>) getInternalTimerService("beam-timer",
        new CoderTypeSerializer<>(timerCoder), this);

    timerInternals = new FlinkTimerInternals();

    // call super at the end because this will call getDoFn() which requires stateInternals
    // to be set
    super.open();
  }

  @Override
  protected ExecutionContext.StepContext createStepContext() {
    return new WindowDoFnOperator.StepContext();
  }

  @Override
  public void onEventTime(InternalTimer<K, TimerData> timer) throws Exception {
    fireTimer(timer);
  }

  @Override
  public void onProcessingTime(InternalTimer<K, TimerData> timer) throws Exception {
    fireTimer(timer);
  }

  public void fireTimer(InternalTimer<K, TimerData> timer) {
    pushbackDoFnRunner.processElement(WindowedValue.valueInGlobalWindow(
        KeyedWorkItems.<K, InputT>timersWorkItem(
            (K) stateInternals.getKey(),
            Collections.singletonList(timer.getNamespace()))));
  }

  @Override
  public void processWatermark1(Watermark mark) throws Exception {
    pushbackDoFnRunner.startBundle();

    this.currentInputWatermark = mark.getTimestamp();

    // hold back by the pushed back values waiting for side inputs
    long actualInputWatermark = Math.min(getPushbackWatermarkHold(), mark.getTimestamp());

    timerService.advanceWatermark(actualInputWatermark);

    Instant watermarkHold = stateInternals.watermarkHold();

    long combinedWatermarkHold = Math.min(watermarkHold.getMillis(), getPushbackWatermarkHold());

    long potentialOutputWatermark = Math.min(currentInputWatermark, combinedWatermarkHold);

    if (potentialOutputWatermark > currentOutputWatermark) {
      currentOutputWatermark = potentialOutputWatermark;
      output.emitWatermark(new Watermark(currentOutputWatermark));
    }
    pushbackDoFnRunner.finishBundle();

  }

  /**
   * {@link StepContext} for running {@link DoFn DoFns} on Flink. This does now allow
   * accessing state or timer internals.
   */
  protected class StepContext extends DoFnOperator.StepContext {

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
      if (timerKey.getDomain().equals(TimeDomain.EVENT_TIME)) {
        timerService.registerEventTimeTimer(timerKey, time);
      } else if (timerKey.getDomain().equals(TimeDomain.PROCESSING_TIME)) {
        timerService.registerProcessingTimeTimer(timerKey, time);
      } else {
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
      if (timerKey.getDomain().equals(TimeDomain.EVENT_TIME)) {
        timerService.deleteEventTimeTimer(timerKey, time);
      } else if (timerKey.getDomain().equals(TimeDomain.PROCESSING_TIME)) {
        timerService.deleteProcessingTimeTimer(timerKey, time);
      } else {
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
