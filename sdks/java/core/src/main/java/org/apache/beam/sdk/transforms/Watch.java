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
package org.apache.beam.sdk.transforms;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.resume;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.stop;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

@Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
public class Watch {
  public static <InputT, OutputT> Growth<InputT, OutputT> growthOf(
      Growth.PollFn<InputT, OutputT> pollFn) {
    return new AutoValue_Watch_Growth.Builder<InputT, OutputT>()
        .setTerminationPerInput(Watch.Growth.never())
        .setPollFn(pollFn)
        .build();
  }

  @AutoValue
  public abstract static class Growth<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollection<KV<InputT, OutputT>>> {
    public static final class PollResult<OutputT> {
      private List<TimestampedValue<OutputT>> outputs;
      private Instant watermark;

      private PollResult(List<TimestampedValue<OutputT>> outputs, Instant watermark) {
        this.outputs = outputs;
        this.watermark = watermark;
      }

      public List<TimestampedValue<OutputT>> getOutputs() {
        return outputs;
      }

      public Instant getWatermark() {
        return watermark;
      }
    }

    public static <OutputT> PollResult<OutputT> outputCanGrow(
        List<TimestampedValue<OutputT>> outputs, Instant watermark) {
      return new PollResult<>(outputs, watermark);
    }

    public static <OutputT> PollResult<OutputT> outputIsFinal(
        List<TimestampedValue<OutputT>> outputs) {
      return new PollResult<>(outputs, BoundedWindow.TIMESTAMP_MAX_VALUE);
    }

    interface PollFn<InputT, OutputT>
        extends SerializableFunction<TimestampedValue<InputT>, PollResult<OutputT>> {}

    interface TerminationCondition extends Serializable {
      TerminationCondition start();

      TerminationCondition onSeenNewOutput();

      boolean canStopPolling();
    }

    public static TerminationCondition never() {
      return new Never();
    }

    public static TerminationCondition afterTotalOf(Duration timeSinceInput) {
      return new AfterTotalOf(timeSinceInput);
    }

    private static class Never implements TerminationCondition {
      @Override
      public TerminationCondition start() {
        return this;
      }

      @Override
      public TerminationCondition onSeenNewOutput() {
        return this;
      }

      @Override
      public boolean canStopPolling() {
        return false;
      }
    }

    private static class AfterTotalOf implements TerminationCondition {
      @Nullable private final Instant timeStarted;
      private final Duration timeSinceInput;

      private AfterTotalOf(Duration timeSinceInput) {
        this.timeStarted = null;
        this.timeSinceInput = timeSinceInput;
      }

      private AfterTotalOf(Instant timeStarted, Duration timeSinceInput) {
        this.timeStarted = checkNotNull(timeStarted);
        this.timeSinceInput = timeSinceInput;
      }

      @Override
      public TerminationCondition start() {
        checkState(timeStarted == null);
        return new AfterTotalOf(Instant.now(), timeSinceInput);
      }

      @Override
      public TerminationCondition onSeenNewOutput() {
        checkState(timeStarted != null);
        return this;
      }

      @Override
      public boolean canStopPolling() {
        checkState(timeStarted != null);
        return new Duration(timeStarted, Instant.now()).isLongerThan(timeSinceInput);
      }
    }

    abstract PollFn<InputT, OutputT> getPollFn();

    @Nullable
    abstract Duration getPollInterval();

    @Nullable
    abstract TerminationCondition getTerminationPerInput();

    @Nullable
    abstract Coder<OutputT> getOutputCoder();

    abstract Builder<InputT, OutputT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<InputT, OutputT> {
      abstract Builder<InputT, OutputT> setPollFn(PollFn<InputT, OutputT> pollFn);

      abstract Builder<InputT, OutputT> setTerminationPerInput(
          TerminationCondition terminationPerInput);

      abstract Builder<InputT, OutputT> setPollInterval(Duration pollInterval);

      abstract Builder<InputT, OutputT> setOutputCoder(Coder<OutputT> outputCoder);

      abstract Growth<InputT, OutputT> build();
    }

    public Growth<InputT, OutputT> withTerminationPerInput(
        TerminationCondition terminationPerInput) {
      return toBuilder().setTerminationPerInput(terminationPerInput).build();
    }

    public Growth<InputT, OutputT> withPollInterval(Duration pollInterval) {
      return toBuilder().setPollInterval(pollInterval).build();
    }

    public Growth<InputT, OutputT> withOutputCoder(Coder<OutputT> outputCoder) {
      return toBuilder().setOutputCoder(outputCoder).build();
    }

    @Override
    public PCollection<KV<InputT, OutputT>> expand(PCollection<InputT> input) {
      checkNotNull(getPollInterval(), "pollInterval");
      checkNotNull(getOutputCoder(), "outputCoder");
      checkNotNull(getTerminationPerInput(), "terminationPerInput");

      return input
          .apply(ParDo.of(new WatchGrowthFn<>(this)))
          .setCoder(KvCoder.of(input.getCoder(), getOutputCoder()));
    }
  }

  private static class WatchGrowthFn<InputT, OutputT> extends DoFn<InputT, KV<InputT, OutputT>> {
    private final Watch.Growth<InputT, OutputT> spec;

    private WatchGrowthFn(Growth<InputT, OutputT> spec) {
      this.spec = spec;
    }

    @ProcessElement
    public ProcessContinuation process(ProcessContext c, final GrowthTracker<OutputT> tracker) {
      Map<OutputT, Instant> pending = tracker.currentRestriction().pending;
      if (pending.isEmpty() && !tracker.currentRestriction().isOutputFinal) {
        Growth.PollResult<OutputT> pollResult =
            spec.getPollFn().apply(TimestampedValue.of(c.element(), c.timestamp()));
        pending = tracker.addNewAsPending(pollResult);
      }
      for (Map.Entry<OutputT, Instant> output : pending.entrySet()) {
        if (!tracker.tryClaim(output.getKey())) {
          return stop();
        }
        c.outputWithTimestamp(KV.of(c.element(), output.getKey()), output.getValue());
      }
      // There are no more pending outputs.
      if (tracker.isOutputFinal()) {
        return stop();
      }
      if (tracker.currentRestriction().terminationCondition.canStopPolling()) {
        return stop();
      }
      return resume().withResumeDelay(spec.getPollInterval());
    }

    @GetInitialRestriction
    public GrowthState<OutputT> getInitialRestriction(InputT element) {
      return new GrowthState<>(spec.getTerminationPerInput().start());
    }

    @GetRestrictionCoder
    public Coder<GrowthState<OutputT>> getRestrictionCoder() {
      return GrowthStateCoder.of(spec.getOutputCoder());
    }
  }

  private static class GrowthState<OutputT>
      implements HasDefaultTracker<GrowthState<OutputT>, GrowthTracker<OutputT>> {
    private final Set<OutputT> completed;
    private final Map<OutputT, Instant> pending;
    private final boolean isOutputFinal;
    // Can be null only if isOutputFinal is true.
    @Nullable private final Growth.TerminationCondition terminationCondition;

    GrowthState(Growth.TerminationCondition terminationCondition) {
      this.completed = Collections.emptySet();
      this.pending = Collections.emptyMap();
      this.isOutputFinal = false;
      this.terminationCondition = checkNotNull(terminationCondition);
    }

    GrowthState(
        Set<OutputT> completed,
        Map<OutputT, Instant> pending,
        boolean isOutputFinal,
        Growth.TerminationCondition terminationCondition) {
      if (!isOutputFinal) {
        checkNotNull(terminationCondition);
      }
      this.completed = Collections.unmodifiableSet(completed);
      this.pending = Collections.unmodifiableMap(pending);
      this.isOutputFinal = isOutputFinal;
      this.terminationCondition = terminationCondition;
    }

    @Override
    public GrowthTracker<OutputT> newTracker() {
      return new GrowthTracker<>(this);
    }
  }

  private static class GrowthTracker<OutputT> implements RestrictionTracker<GrowthState<OutputT>> {
    // Invariant: only one of {state.pending, this.newPending} is non-empty.
    private GrowthState<OutputT> state;
    private final Map<OutputT, Instant> newPending = Maps.newHashMap();
    private final Map<OutputT, Instant> claimed = Maps.newHashMap();
    private boolean isOutputFinal;
    private Growth.TerminationCondition terminationCondition;
    private boolean shouldStop = false;

    private GrowthTracker(GrowthState<OutputT> state) {
      this.state = state;
      this.isOutputFinal = state.isOutputFinal;
      this.terminationCondition = state.terminationCondition;
    }

    @Override
    public synchronized GrowthState<OutputT> currentRestriction() {
      return state;
    }

    @Override
    public synchronized GrowthState<OutputT> checkpoint() {
      // primary should contain exactly the work claimed in the current ProcessElement call - i.e.
      // claimed outputs become pending, and it shouldn't poll again.
      GrowthState<OutputT> primary =
          new GrowthState<>(
              state.completed /* completed */,
              claimed /* pending */,
              true /* isOutputFinal */,
              null /* terminationCondition */);

      // residual should contain exactly the work *not* claimed in the current ProcessElement call -
      // it should assume that work claimed previously or claimed by the current call is completed;
      // and pending work is union of previously or newly known pending work, minus completed.

      // residualPending = pending - claimed.
      checkState(state.pending.isEmpty() || newPending.isEmpty());
      Map<OutputT, Instant> residualPending =
          Maps.newHashMap(state.pending.isEmpty() ? newPending : state.pending);
      residualPending.keySet().removeAll(claimed.keySet());
      GrowthState<OutputT> residual =
          new GrowthState<>(
              Sets.union(state.completed, claimed.keySet()) /* completed */,
              residualPending /* pending */,
              isOutputFinal /* isOutputFinal */,
              terminationCondition);
      this.state = primary;
      this.shouldStop = true;
      return residual;
    }

    @Override
    public void checkDone() throws IllegalStateException {
      if (shouldStop) return;
      checkState(isOutputFinal(), "Polling is still allowed to continue");
      checkState(state.pending.isEmpty(), "There are %s old pending outputs", state.pending.size());
      checkState(newPending.isEmpty(), "There are %s new pending outputs", newPending.size());
    }

    private synchronized boolean tryClaim(OutputT pendingValue) {
      checkState(
          !claimed.containsKey(pendingValue),
          "Value %s already claimed in the current call",
          pendingValue);
      checkState(
          !state.completed.contains(pendingValue),
          "Value %s already previously claimed",
          pendingValue);
      checkState(
          state.pending.containsKey(pendingValue) || newPending.containsKey(pendingValue),
          "Value %s was not added to pending values",
          pendingValue);
      if (shouldStop) {
        return false;
      }
      Instant timestamp = newPending.remove(pendingValue);
      claimed.put(pendingValue, timestamp);
      return true;
    }

    private synchronized boolean isOutputFinal() {
      return isOutputFinal || terminationCondition.canStopPolling();
    }

    public synchronized Map<OutputT, Instant> addNewAsPending(
        Growth.PollResult<OutputT> pollResult) {
      for (TimestampedValue<OutputT> output : pollResult.getOutputs()) {
        OutputT value = output.getValue();
        if (state.completed.contains(value)
            || state.pending.containsKey(value)
            || newPending.containsKey(value)) {
          continue;
        }
        if (newPending.isEmpty()) {
          terminationCondition = terminationCondition.onSeenNewOutput();
        }
        newPending.put(value, output.getTimestamp());
      }
      if (pollResult.getWatermark().equals(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
        isOutputFinal = true;
      } else {
        // TODO update watermark
        // Watermark = min(remaining pending elements, future output)
        // Changes when adding a pending element, when tryClaim'ing a pending element, or when
        // getting a new promise on future output.
      }
      return ImmutableMap.copyOf(newPending);
    }
  }

  private static class GrowthStateCoder<OutputT> extends StructuredCoder<GrowthState<OutputT>> {
    public static <OutputT> GrowthStateCoder<OutputT> of(Coder<OutputT> outputCoder) {
      return new GrowthStateCoder<>(outputCoder);
    }

    private final Coder<OutputT> outputCoder;
    private final SetCoder<OutputT> completedCoder;
    private final MapCoder<OutputT, Instant> pendingCoder;
    private final VarIntCoder isOutputFinalCoder = VarIntCoder.of();
    private final SerializableCoder<Growth.TerminationCondition> terminationConditionCoder =
        SerializableCoder.of(Growth.TerminationCondition.class);

    private GrowthStateCoder(Coder<OutputT> outputCoder) {
      this.outputCoder = outputCoder;
      this.completedCoder = SetCoder.of(outputCoder);
      this.pendingCoder = MapCoder.of(outputCoder, InstantCoder.of());
    }

    @Override
    public void encode(GrowthState<OutputT> value, OutputStream os) throws IOException {
      completedCoder.encode(value.completed, os);
      pendingCoder.encode(value.pending, os);
      isOutputFinalCoder.encode(value.isOutputFinal ? 1 : 0, os);
      terminationConditionCoder.encode(value.terminationCondition, os);
    }

    @Override
    public GrowthState<OutputT> decode(InputStream is) throws IOException {
      Set<OutputT> completed = completedCoder.decode(is);
      Map<OutputT, Instant> pending = pendingCoder.decode(is);
      boolean isOutputFinal = (isOutputFinalCoder.decode(is) == 1);
      Growth.TerminationCondition terminationCondition = terminationConditionCoder.decode(is);
      return new GrowthState<>(completed, pending, isOutputFinal, terminationCondition);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(outputCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      outputCoder.verifyDeterministic();
    }
  }
}
