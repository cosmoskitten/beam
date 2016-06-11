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

import static java.lang.System.out;

import org.apache.beam.runners.flink.translation.wrappers.DataInputViewWrapper;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.StateCheckpointReader;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.StateCheckpointUtils;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.StateCheckpointWriter;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.util.ExecutionContext;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.KeyedWorkItems;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.joda.time.Instant;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Flink operator for executing window {@link DoFn DoFns}.
 *
 * @param <InputT>
 * @param <FnOutputT>
 * @param <OutputT>
 */
public class WindowDoFnOperator<K, InputT, FnOutputT, OutputT>
    extends DoFnOperator<KeyedWorkItem<K, InputT>, FnOutputT, OutputT> {

  /**
   * To keep track of the current watermark so that we can immediately fire if a trigger
   * registers an event time callback for a timestamp that lies in the past.
   */
  protected transient long currentWatermark = Long.MIN_VALUE;

  private final Coder<K> keyCoder;
  private final TimerInternals.TimerDataCoder timerCoder;
  private final Coder<? extends BoundedWindow> windowCoder;
  private final OutputTimeFn<? extends BoundedWindow> outputTimeFn;

  private Map<K, FlinkStateInternals<K>> perKeyStateInternals = new HashMap<>();

  private transient Set<Tuple2<K, TimerInternals.TimerData>> watermarkTimers;
  private transient Queue<Tuple2<K, TimerInternals.TimerData>> watermarkTimersQueue;

  public WindowDoFnOperator(
      DoFn<KeyedWorkItem<K, InputT>, FnOutputT> doFn,
      TupleTag<FnOutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      OutputManagerFactory<OutputT> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      PipelineOptions options,
      Coder<K> keyCoder) {
    super(
        doFn,
        mainOutputTag,
        sideOutputTags,
        outputManagerFactory,
        windowingStrategy,
        sideInputs,
        options);
    this.keyCoder = keyCoder;
    this.timerCoder =
        TimerInternals.TimerDataCoder.of(windowingStrategy.getWindowFn().windowCoder());
    this.windowCoder = windowingStrategy.getWindowFn().windowCoder();
    this.outputTimeFn = windowingStrategy.getOutputTimeFn();
  }


  @Override
  public void open() throws Exception {
    super.open();

    // might already be initialized from restoreTimers()
    if (watermarkTimers == null) {
      watermarkTimers = new HashSet<>();

      watermarkTimersQueue = new PriorityQueue<>(
          10,
          new Comparator<Tuple2<K, TimerInternals.TimerData>>() {
            @Override
            public int compare(
                Tuple2<K, TimerInternals.TimerData> o1,
                Tuple2<K, TimerInternals.TimerData> o2) {
              return o1.f1.compareTo(o2.f1);
            }
          });
    }

  }

  @Override
  protected ExecutionContext.StepContext createStepContext() {
    return new WindowDoFnOperator.StepContext();
  }

  private void registerEventTimeTimer(TimerInternals.TimerData timer) {
    Tuple2<K, TimerInternals.TimerData> keyedTimer =
        new Tuple2<>((K) getStateBackend().getCurrentKey(), timer);
    if (watermarkTimers.add(keyedTimer)) {
      out.println("ADD TIMER " + timer);
      watermarkTimersQueue.add(keyedTimer);
    }
  }

  private void deleteEventTimeTimer(TimerInternals.TimerData timer) {
    Tuple2<K, TimerInternals.TimerData> keyedTimer =
        new Tuple2<>((K) getStateBackend().getCurrentKey(), timer);
    if (watermarkTimers.remove(timer)) {
      watermarkTimersQueue.remove(timer);
    }

  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    this.currentWatermark = mark.getTimestamp();

    boolean fire;

    do {
      Tuple2<K, TimerInternals.TimerData> timer = watermarkTimersQueue.peek();
      if (timer != null && timer.f1.getTimestamp().getMillis() <= mark.getTimestamp()) {
        fire = true;

        out.println("FIRE TIMER: " + timer);

        watermarkTimersQueue.remove();
        watermarkTimers.remove(timer);

        setKeyContext(timer.f0);

        doFnRunner.processElement(WindowedValue.valueInGlobalWindow(
                KeyedWorkItems.<K, InputT>timersWorkItem(
                    timer.f0,
                    Collections.singletonList(timer.f1))));

      } else {
        fire = false;
      }
    } while (fire);

    output.emitWatermark(mark);
  }

  /**
   * Gets the state associated with the specified key.
   *
   * @param key the key whose state we want.
   * @return The {@link FlinkStateInternals}
   * associated with that key.
   */
  private FlinkStateInternals<K> getStateInternalsForKey(K key) {
    FlinkStateInternals<K> stateInternals = perKeyStateInternals.get(key);
    if (stateInternals == null) {
      Coder<? extends BoundedWindow> windowCoder =
          this.windowingStrategy.getWindowFn().windowCoder();

      @SuppressWarnings("unchecked")
      OutputTimeFn<? super BoundedWindow> outputTimeFn =
          (OutputTimeFn<? super BoundedWindow>) windowingStrategy.getOutputTimeFn();

      stateInternals = new FlinkStateInternals<>(key, windowCoder, outputTimeFn);

      perKeyStateInternals.put(key, stateInternals);
    }
    return stateInternals;
  }

  @Override
  public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
    StreamTaskState result = super.snapshotOperatorState(checkpointId, timestamp);

    AbstractStateBackend.CheckpointStateOutputView outputView =
        getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

    snapshotTimers(outputView);

    StateCheckpointWriter writer = StateCheckpointWriter.create(outputView);

    StateCheckpointUtils.encodeState(perKeyStateInternals, writer, keyCoder);

    StateHandle<DataInputView> handle = outputView.closeAndGetHandle();

    // this might overwrite stuff that super checkpointed
    result.setOperatorState(handle);

    return result;
  }

  @Override
  public void restoreState(StreamTaskState state, long recoveryTimestamp) throws Exception {
    super.restoreState(state, recoveryTimestamp);

    @SuppressWarnings("unchecked")
    StateHandle<DataInputView> operatorState =
        (StateHandle<DataInputView>) state.getOperatorState();

    DataInputView in = operatorState.getState(getUserCodeClassloader());

    restoreTimers(new DataInputViewWrapper(in));

    StateCheckpointReader reader = new StateCheckpointReader(in);
    StateCheckpointUtils.decodeState(
        reader,
        (OutputTimeFn) outputTimeFn,
        keyCoder,
        windowCoder,
        getUserCodeClassloader());

  }

  private void restoreTimers(InputStream in) throws IOException {
    DataInputStream dataIn = new DataInputStream(in);
    int numWatermarkTimers = dataIn.readInt();

    watermarkTimers = new HashSet<>(numWatermarkTimers);
    watermarkTimersQueue = new PriorityQueue<>(Math.max(numWatermarkTimers, 1));

    for (int i = 0; i < numWatermarkTimers; i++) {
      K key = keyCoder.decode(dataIn, Coder.Context.NESTED);
      TimerInternals.TimerData timerData = timerCoder.decode(dataIn, Coder.Context.NESTED);
      Tuple2<K, TimerInternals.TimerData> keyedTimer = new Tuple2<>(key, timerData);
      if (watermarkTimers.add(keyedTimer)) {
        watermarkTimersQueue.add(keyedTimer);
      }
    }
  }

  private void snapshotTimers(OutputStream out) throws IOException {
    DataOutputStream dataOut = new DataOutputStream(out);
    dataOut.writeInt(watermarkTimersQueue.size());
    for (Tuple2<K, TimerInternals.TimerData> timer : watermarkTimersQueue) {
      keyCoder.encode(timer.f0, dataOut, Coder.Context.NESTED);
      timerCoder.encode(timer.f1, dataOut, Coder.Context.NESTED);
    }
  }

  /**
   * {@link StepContext} for running {@link DoFn DoFns} on Flink. This does now allow
   * accessing state or timer internals.
   */
  protected class StepContext extends DoFnOperator.StepContext {

    @Override
    public StateInternals<?> stateInternals() {
      @SuppressWarnings("unchecked")
      K key = (K) getStateBackend().getCurrentKey();
      return getStateInternalsForKey(key);
    }

    @Override
    public TimerInternals timerInternals() {
      return new TimerInternals() {
        @Override
        public void setTimer(TimerData timerKey) {
          if (timerKey.getDomain().equals(TimeDomain.EVENT_TIME)) {
            registerEventTimeTimer(timerKey);
          } else {
            throw new UnsupportedOperationException("Processing-time timer not supported.");
          }
        }

        @Override
        public void deleteTimer(TimerData timerKey) {
          deleteEventTimeTimer(timerKey);
        }

        @Override
        public Instant currentProcessingTime() {
          return Instant.now();
        }

        @Nullable
        @Override
        public Instant currentSynchronizedProcessingTime() {
          return Instant.now();
        }

        @Override
        public Instant currentInputWatermarkTime() {
          return new Instant(currentWatermark);
        }

        @Nullable
        @Override
        public Instant currentOutputWatermarkTime() {
          return new Instant(currentWatermark);
        }
      };
    }
  }

}
