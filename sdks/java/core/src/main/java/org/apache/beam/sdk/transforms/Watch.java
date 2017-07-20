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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.resume;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.stop;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a "poll function" that produces a potentially growing set of outputs for an input, this
 * transform simultaneously continuously watches the growth of output sets of all inputs, until a
 * per-input termination condition is reached.
 *
 * <p>The output is returned as an unbounded {@link PCollection} of {@code KV<InputT, OutputT>},
 * where each {@code OutputT} is associated with the {@code InputT} that produced it, and is
 * assigned with the timestamp that the poll function returned when this output was detected for the
 * first time.
 *
 * <p>Hypothetical usage example for watching new files in a collection of directories, where for
 * each directory we assume that new files will not appear if the directory contains a file named
 * ".complete":
 *
 * <pre>{@code
 * PCollection<String> directories = ...;  // E.g. Create.of(single directory)
 * PCollection<KV<String, String>> matches = filepatterns.apply(Watch.<String, String>growthOf(
 *   new PollFn<String, String>() {
 *     public PollResult<String> apply(TimestampedValue<String> input) {
 *       String directory = input.getValue();
 *       List<TimestampedValue<String>> outputs = new ArrayList<>();
 *       ... List the directory and get creation times of all files ...
 *       boolean isComplete = ... does a file ".complete" exist in the directory ...
 *       return isComplete ? PollResult.complete(outputs) : PollResult.incomplete(outputs);
 *     }
 *   })
 *   // Poll each directory every 5 seconds
 *   .withPollInterval(Duration.standardSeconds(5))
 *   // Stop watching each directory 12 hours after it's seen even if it's incomplete
 *   .withTerminationPerInput(afterTotalOf(Duration.standardHours(12)));
 * }</pre>
 *
 * <h4>Watermarks</h4>
 *
 * <p>By default, the watermark for a particular input is computed from a poll result as "earliest
 * timestamp of new elements in this poll result". It can also be set explicitly via {@link
 * Growth.PollResult#withWatermark} if the {@link Growth.PollFn} can provide a more optimistic
 * estimate.
 *
 * <p>Note: This transform works only in runners supporting Splittable DoFn: see <a
 * href="https://beam.apache.org/documentation/runners/capability-matrix/">capability matrix</a>.
 */
@Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
public class Watch {
  private static final Logger LOG = LoggerFactory.getLogger(Watch.class);

  /** Watches the growth of the given poll function. See class documentation for more details. */
  public static <InputT, OutputT> Growth<InputT, OutputT> growthOf(
      Growth.PollFn<InputT, OutputT> pollFn) {
    return new AutoValue_Watch_Growth.Builder<InputT, OutputT>()
        .setTerminationPerInput(Watch.Growth.never())
        .setPollFn(pollFn)
        .build();
  }

  /** Implementation of {@link #growthOf}. */
  @AutoValue
  public abstract static class Growth<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollection<KV<InputT, OutputT>>> {
    /** The result of a single invocation of a {@link PollFn}. */
    public static final class PollResult<OutputT> {
      private final List<TimestampedValue<OutputT>> outputs;
      // null means unspecified (infer automatically).
      @Nullable private final Instant watermark;

      private PollResult(List<TimestampedValue<OutputT>> outputs, @Nullable Instant watermark) {
        this.outputs = outputs;
        this.watermark = watermark;
      }

      List<TimestampedValue<OutputT>> getOutputs() {
        return outputs;
      }

      @Nullable
      Instant getWatermark() {
        return watermark;
      }

      /**
       * Sets the watermark - an approximate lower bound on timestamps of future new outputs from
       * this {@link PollFn}.
       */
      public PollResult<OutputT> withWatermark(Instant watermark) {
        checkNotNull(watermark, "watermark");
        return new PollResult<>(outputs, watermark);
      }

      /**
       * Constructs a {@link PollResult} with the given outputs and declares that there will be no
       * new outputs for the current input. The {@link PollFn} will not be called again for this
       * input.
       */
      public static <OutputT> PollResult<OutputT> complete(
          List<TimestampedValue<OutputT>> outputs) {
        return new PollResult<>(outputs, BoundedWindow.TIMESTAMP_MAX_VALUE);
      }

      /** Like {@link #complete(List)}, but assigns the same timestamp to all new outputs. */
      public static <OutputT> PollResult<OutputT> complete(
          Instant timestamp, List<OutputT> outputs) {
        return new PollResult<>(
            addTimestamp(timestamp, outputs), BoundedWindow.TIMESTAMP_MAX_VALUE);
      }

      /**
       * Constructs a {@link PollResult} with the given outputs and declares that new outputs might
       * appear for the current input.
       */
      public static <OutputT> PollResult<OutputT> incomplete(
          List<TimestampedValue<OutputT>> outputs) {
        return new PollResult<>(outputs, null);
      }

      /** Like {@link #incomplete(List)}, but assigns the same timestamp to all new outputs. */
      public static <OutputT> PollResult<OutputT> incomplete(
          Instant timestamp, List<OutputT> outputs) {
        return new PollResult<>(addTimestamp(timestamp, outputs), null);
      }

      private static <OutputT> List<TimestampedValue<OutputT>> addTimestamp(
          Instant timestamp, List<OutputT> outputs) {
        List<TimestampedValue<OutputT>> res = Lists.newArrayList();
        for (OutputT output : outputs) {
          res.add(TimestampedValue.of(output, timestamp));
        }
        return res;
      }
    }

    /**
     * A function that computes the current set of outputs for the given input (given as a {@link
     * TimestampedValue}), in the form of a {@link PollResult}.
     */
    interface PollFn<InputT, OutputT>
        extends SerializableFunction<TimestampedValue<InputT>, PollResult<OutputT>> {}

    /**
     * An object that determines whether it is time to stop polling the current input regardless of
     * whether its output is complete or not.
     *
     * <p>A single instance of {@link TerminationCondition} is supplied to {@link
     * Growth#withTerminationPerInput(TerminationCondition)} and is used as an initial value for
     * every input. Each input maintains its own independent {@link TerminationCondition}.
     *
     * <p>{@link TerminationCondition} objects must be immutable and serializable. State must be
     * carried over by returning new immutable instances of {@link TerminationCondition} rather than
     * modifying the current instance.
     *
     * <p>All functions take the wall-clock timestamp as {@link Instant} for convenience of
     * unit-testing custom termination conditions.
     */
    interface TerminationCondition extends Serializable {
      /**
       * Called by the {@link Watch} transform to create a new independent instance of this {@link
       * TerminationCondition} for a newly arrived {@code InputT}.
       */
      TerminationCondition forNewInput(Instant now);

      /**
       * Called by the {@link Watch} transform when, after calling the {@link PollFn} for the
       * current input, the {@link PollResult} included a previously unseen {@code OutputT}.
       */
      TerminationCondition onSeenNewOutput(Instant now);

      /**
       * Called by the {@link Watch} transform to determine whether it should stop calling {@link
       * PollFn} for the current input, regardless of whether the last {@link PollResult} was
       * complete or incomplete.
       */
      boolean canStopPolling(Instant now);
    }

    /**
     * Returns a {@link TerminationCondition} that never holds (i.e., poll each input until its
     * output is complete).
     */
    public static TerminationCondition never() {
      return new Never();
    }

    /**
     * Returns a {@link TerminationCondition} that holds after the given time has elapsed after the
     * current input was seen.
     */
    public static TerminationCondition afterTotalOf(Duration timeSinceInput) {
      return new AfterTotalOf(timeSinceInput);
    }

    /**
     * Returns a {@link TerminationCondition} that holds after the given time has elapsed after the
     * last time the {@link PollResult} for the current input contained a previously unseen output.
     */
    public static TerminationCondition afterTimeSinceNewOutput(Duration timeSinceNewOutput) {
      return new AfterTimeSinceNewOutput(timeSinceNewOutput);
    }

    /**
     * Returns a {@link TerminationCondition} that holds when at least one of the given two
     * conditions holds.
     */
    public static TerminationCondition eitherOf(
        TerminationCondition first, TerminationCondition second) {
      return new BinaryCombined(BinaryCombined.Operation.OR, first, second);
    }

    /**
     * Returns a {@link TerminationCondition} that holds when both of the given two conditions hold.
     */
    public static TerminationCondition allOf(
        TerminationCondition first, TerminationCondition second) {
      return new BinaryCombined(BinaryCombined.Operation.AND, first, second);
    }

    private static class Never implements TerminationCondition {
      @Override
      public TerminationCondition forNewInput(Instant now) {
        return this;
      }

      @Override
      public TerminationCondition onSeenNewOutput(Instant now) {
        return this;
      }

      @Override
      public boolean canStopPolling(Instant now) {
        return false;
      }

      @Override
      public String toString() {
        return "Never{}";
      }
    }

    private static class AfterTotalOf implements TerminationCondition {
      @Nullable private final Instant timeStarted;
      private final Duration maxTimeSinceInput;

      private AfterTotalOf(Duration maxTimeSinceInput) {
        this.timeStarted = null;
        this.maxTimeSinceInput = maxTimeSinceInput;
      }

      private AfterTotalOf(Instant timeStarted, Duration maxTimeSinceInput) {
        this.timeStarted = checkNotNull(timeStarted);
        this.maxTimeSinceInput = maxTimeSinceInput;
      }

      @Override
      public TerminationCondition forNewInput(Instant now) {
        checkState(timeStarted == null);
        return new AfterTotalOf(now, maxTimeSinceInput);
      }

      @Override
      public TerminationCondition onSeenNewOutput(Instant now) {
        checkState(timeStarted != null);
        return this;
      }

      @Override
      public boolean canStopPolling(Instant now) {
        checkState(timeStarted != null);
        return new Duration(timeStarted, now).isLongerThan(maxTimeSinceInput);
      }

      @Override
      public String toString() {
        return "AfterTotalOf{"
            + "timeStarted="
            + timeStarted
            + ", maxTimeSinceInput="
            + maxTimeSinceInput
            + '}';
      }
    }

    private static class AfterTimeSinceNewOutput implements TerminationCondition {
      @Nullable private final Instant timeOfLastNewOutput;
      private final Duration maxTimeSinceNewOutput;

      private AfterTimeSinceNewOutput(Duration maxTimeSinceNewOutput) {
        this.timeOfLastNewOutput = null;
        this.maxTimeSinceNewOutput = maxTimeSinceNewOutput;
      }

      private AfterTimeSinceNewOutput(Instant timeOfLastNewOutput, Duration maxTimeSinceNewOutput) {
        this.timeOfLastNewOutput = checkNotNull(timeOfLastNewOutput);
        this.maxTimeSinceNewOutput = maxTimeSinceNewOutput;
      }

      @Override
      public TerminationCondition forNewInput(Instant now) {
        return this;
      }

      @Override
      public TerminationCondition onSeenNewOutput(Instant now) {
        return new AfterTimeSinceNewOutput(now, maxTimeSinceNewOutput);
      }

      @Override
      public boolean canStopPolling(Instant now) {
        return timeOfLastNewOutput != null
            && new Duration(timeOfLastNewOutput, now).isLongerThan(maxTimeSinceNewOutput);
      }

      @Override
      public String toString() {
        return "AfterTimeSinceNewOutput{"
            + "timeOfLastNewOutput="
            + timeOfLastNewOutput
            + ", maxTimeSinceNewOutput="
            + maxTimeSinceNewOutput
            + '}';
      }
    }

    private static class BinaryCombined implements TerminationCondition {
      private enum Operation {
        OR,
        AND
      }

      private final Operation operation;
      private final TerminationCondition first;
      private final TerminationCondition second;

      public BinaryCombined(
          Operation operation, TerminationCondition first, TerminationCondition second) {
        this.operation = operation;
        this.first = first;
        this.second = second;
      }

      @Override
      public TerminationCondition forNewInput(Instant now) {
        return new BinaryCombined(operation, first.forNewInput(now), second.forNewInput(now));
      }

      @Override
      public TerminationCondition onSeenNewOutput(Instant now) {
        return new BinaryCombined(
            operation, first.onSeenNewOutput(now), second.onSeenNewOutput(now));
      }

      @Override
      public boolean canStopPolling(Instant now) {
        switch (operation) {
          case OR:
            return first.canStopPolling(now) || second.canStopPolling(now);
          case AND:
            return first.canStopPolling(now) && second.canStopPolling(now);
          default:
            throw new UnsupportedOperationException("Unexpected operation " + operation);
        }
      }

      @Override
      public String toString() {
        return operation + "{first=" + first + ", second=" + second + '}';
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

    /** Specifies a {@link TerminationCondition} that will be independently used for every input. */
    public Growth<InputT, OutputT> withTerminationPerInput(
        TerminationCondition terminationPerInput) {
      return toBuilder().setTerminationPerInput(terminationPerInput).build();
    }

    /**
     * Specifies how long to wait after a call to {@link PollFn} before calling it again (if at all
     * - according to {@link PollResult} and the {@link TerminationCondition}).
     */
    public Growth<InputT, OutputT> withPollInterval(Duration pollInterval) {
      return toBuilder().setPollInterval(pollInterval).build();
    }

    /**
     * Specifies a {@link Coder} to use for the outputs. If unspecified, it will be inferred from
     * the output type of {@link PollFn} whenever possible.
     *
     * <p>The coder must be deterministic, because the transform will compare encoded outputs
     * for deduplication between polling rounds.
     */
    public Growth<InputT, OutputT> withOutputCoder(Coder<OutputT> outputCoder) {
      return toBuilder().setOutputCoder(outputCoder).build();
    }

    @Override
    public PCollection<KV<InputT, OutputT>> expand(PCollection<InputT> input) {
      checkNotNull(getPollInterval(), "pollInterval");
      checkNotNull(getTerminationPerInput(), "terminationPerInput");

      Coder<OutputT> outputCoder = getOutputCoder();
      if (outputCoder == null) {
        // If a coder was not specified explicitly, infer it from the OutputT type parameter
        // of the PollFn.
        TypeDescriptor<?> superDescriptor =
            TypeDescriptor.of(getPollFn().getClass()).getSupertype(PollFn.class);
        TypeVariable typeVariable = superDescriptor.getTypeParameter("OutputT");
        @SuppressWarnings("unchecked")
        TypeDescriptor<OutputT> descriptor =
            (TypeDescriptor<OutputT>) superDescriptor.resolveType(typeVariable);
        try {
          outputCoder = input.getPipeline().getCoderRegistry().getCoder(descriptor);
        } catch (CannotProvideCoderException e) {
          throw new RuntimeException(
              "Unable to infer coder for OutputT. Specify it explicitly using withOutputCoder().");
        }
      }
      try {
        outputCoder.verifyDeterministic();
      } catch (Coder.NonDeterministicException e) {
        throw new IllegalArgumentException(
            "Output coder " + outputCoder + " must be deterministic");
      }

      return input
          .apply(ParDo.of(new WatchGrowthFn<>(this, outputCoder)))
          .setCoder(KvCoder.of(input.getCoder(), outputCoder));
    }
  }

  private static class WatchGrowthFn<InputT, OutputT> extends DoFn<InputT, KV<InputT, OutputT>> {
    private final Watch.Growth<InputT, OutputT> spec;
    private final Coder<OutputT> outputCoder;

    private WatchGrowthFn(Growth<InputT, OutputT> spec, Coder<OutputT> outputCoder) {
      this.spec = spec;
      this.outputCoder = outputCoder;
    }

    @ProcessElement
    public ProcessContinuation process(ProcessContext c, final GrowthTracker<OutputT> tracker) {
      if (!tracker.hasPending() && !tracker.currentRestriction().isOutputComplete) {
        LOG.info("{} - polling input", c.element());
        Growth.PollResult<OutputT> res =
            spec.getPollFn().apply(TimestampedValue.of(c.element(), c.timestamp()));
        // TODO: Consider truncating the pending outputs if there are too many, to avoid blowing
        // up the state. In that case, we'd rely on the next poll cycle to provide more outputs.
        // All outputs would still have to be stored in state.completed, but it is more compact
        // because it stores hashes and because it could potentially be garbage-collected.
        int numPending = tracker.addNewAsPending(res);
        LOG.info(
            "{} - polling returned {} results, of which {} were new. The output is {}.",
            c.element(),
            res.getOutputs().size(),
            numPending,
            BoundedWindow.TIMESTAMP_MAX_VALUE.equals(res.getWatermark())
                ? "complete"
                : "incomplete");
      }
      while (tracker.hasPending()) {
        c.updateWatermark(tracker.getWatermark());

        TimestampedValue<OutputT> nextPending = tracker.tryClaimNextPending();
        if (nextPending == null) {
          return stop();
        }
        c.outputWithTimestamp(
            KV.of(c.element(), nextPending.getValue()), nextPending.getTimestamp());
      }
      Instant watermark = tracker.getWatermark();
      if (watermark != null) {
        // Null means the poll result did not provide a watermark and there were no new elements,
        // so we have no information to update the watermark and should keep it as-is.
        c.updateWatermark(watermark);
      }
      // No more pending outputs - future output will come from more polling,
      // unless output is complete or termination condition is reached.
      if (tracker.shouldPollMore()) {
        return resume().withResumeDelay(spec.getPollInterval());
      }
      return stop();
    }

    @GetInitialRestriction
    public GrowthState<OutputT> getInitialRestriction(InputT element) {
      return new GrowthState<>(spec.getTerminationPerInput().forNewInput(Instant.now()));
    }

    @NewTracker
    public GrowthTracker<OutputT> newTracker(GrowthState<OutputT> restriction) {
      return new GrowthTracker<>(outputCoder, restriction);
    }

    @GetRestrictionCoder
    public Coder<GrowthState<OutputT>> getRestrictionCoder() {
      return GrowthStateCoder.of(outputCoder);
    }
  }

  @VisibleForTesting
  static class GrowthState<OutputT> {
    // Hashes and timestamps of outputs that have already been output and should be omitted
    // from future polls. Timestamps are preserved to allow garbage-collecting this state
    // in the future, e.g. dropping elements from "completed" and from addNewAsPending() if their
    // timestamp is more than X behind the watermark.
    // As of writing, we don't do this, but preserve the information for forward compatibility
    // in case of pipeline update. TODO: do this.
    private final Map<HashCode, Instant> completed;
    // Outputs that are known to be present in a poll result, but have not yet been returned
    // from a ProcessElement call, sorted by timestamp to help smooth watermark progress.
    private final List<TimestampedValue<OutputT>> pending;
    // If true, processing of this restriction should only output "pending". Otherwise, it should
    // also continue polling.
    private final boolean isOutputComplete;
    // Can be null only if isOutputComplete is true.
    @Nullable private final Growth.TerminationCondition terminationCondition;
    // A lower bound on timestamps of future outputs from PollFn, excluding completed and pending.
    private final Instant pollWatermark;

    GrowthState(Growth.TerminationCondition terminationCondition) {
      this.completed = Collections.emptyMap();
      this.pending = Collections.emptyList();
      this.isOutputComplete = false;
      this.terminationCondition = checkNotNull(terminationCondition);
      this.pollWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    GrowthState(
        Map<HashCode, Instant> completed,
        List<TimestampedValue<OutputT>> pending,
        boolean isOutputComplete,
        Growth.TerminationCondition terminationCondition,
        Instant pollWatermark) {
      if (!isOutputComplete) {
        checkNotNull(terminationCondition);
      }
      this.completed = Collections.unmodifiableMap(completed);
      this.pending = Collections.unmodifiableList(pending);
      this.isOutputComplete = isOutputComplete;
      this.terminationCondition = terminationCondition;
      this.pollWatermark = pollWatermark;
    }

    @Override
    public String toString() {
      return "GrowthState{"
          + "completed=<"
          + completed.size()
          + " elements>, pending=<"
          + pending.size()
          + " elements"
          + (pending.isEmpty() ? "" : (", earliest " + pending.get(0)))
          + ">, isOutputComplete="
          + isOutputComplete
          + ", terminationCondition="
          + terminationCondition
          + ", pollWatermark="
          + pollWatermark
          + '}';
    }
  }

  @VisibleForTesting
  static class GrowthTracker<OutputT> implements RestrictionTracker<GrowthState<OutputT>> {
    private final Funnel<OutputT> coderFunnel;

    // The restriction describing the entire work to be done by the current ProcessElement call.
    // Changes only in checkpoint().
    private GrowthState<OutputT> state;

    // Mutable state changed by the ProcessElement call itself, and used to compute the primary
    // and residual restrictions in checkpoint().

    // Remaining pending outputs; initialized from state.pending (if non-empty) or in
    // addNewAsPending(); drained via tryClaimNextPending().
    private LinkedList<TimestampedValue<OutputT>> pending;
    // Outputs that have been claimed in the current ProcessElement call. A prefix of "pending".
    private List<TimestampedValue<OutputT>> claimed = Lists.newArrayList();
    private boolean isOutputComplete;
    private Growth.TerminationCondition terminationCondition;
    private Instant pollWatermark;
    private boolean shouldStop = false;

    GrowthTracker(final Coder<OutputT> outputCoder, GrowthState<OutputT> state) {
      this.coderFunnel =
          new Funnel<OutputT>() {
            @Override
            public void funnel(OutputT from, PrimitiveSink into) {
              try {
                outputCoder.encode(from, Funnels.asOutputStream(into));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          };
      this.state = state;
      this.isOutputComplete = state.isOutputComplete;
      this.pollWatermark = state.pollWatermark;
      this.terminationCondition = state.terminationCondition;
      this.pending = Lists.newLinkedList(state.pending);
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
              true /* isOutputComplete */,
              null /* terminationCondition */,
              BoundedWindow.TIMESTAMP_MAX_VALUE /* pollWatermark */);

      // residual should contain exactly the work *not* claimed in the current ProcessElement call -
      // unclaimed pending outputs plus future polling outputs.
      Map<HashCode, Instant> newCompleted = Maps.newHashMap(state.completed);
      for (TimestampedValue<OutputT> claimedOutput : claimed) {
        newCompleted.put(hash128(claimedOutput.getValue()), claimedOutput.getTimestamp());
      }
      GrowthState<OutputT> residual =
          new GrowthState<>(
              newCompleted /* completed */,
              pending /* pending */,
              isOutputComplete /* isOutputComplete */,
              terminationCondition,
              pollWatermark);

      // Morph ourselves into primary, except for "pending" - the current call has already claimed
      // everything from it.
      this.state = primary;
      this.isOutputComplete = primary.isOutputComplete;
      this.pollWatermark = primary.pollWatermark;
      this.terminationCondition = null;
      this.pending = Lists.newLinkedList();

      this.shouldStop = true;
      return residual;
    }

    private HashCode hash128(OutputT value) {
      return Hashing.murmur3_128().hashObject(value, coderFunnel);
    }

    @Override
    public synchronized void checkDone() throws IllegalStateException {
      if (shouldStop) {
        return;
      }
      checkState(!shouldPollMore(), "Polling is still allowed to continue");
      checkState(pending.isEmpty(), "There are %s unclaimed pending outputs", pending.size());
    }

    @VisibleForTesting
    synchronized boolean hasPending() {
      return !pending.isEmpty();
    }

    @VisibleForTesting
    @Nullable
    synchronized TimestampedValue<OutputT> tryClaimNextPending() {
      if (shouldStop) {
        return null;
      }
      checkState(!pending.isEmpty(), "No more unclaimed pending outputs");
      TimestampedValue<OutputT> value = pending.removeFirst();
      claimed.add(value);
      return value;
    }

    @VisibleForTesting
    synchronized boolean shouldPollMore() {
      return !isOutputComplete && !terminationCondition.canStopPolling(Instant.now());
    }

    @VisibleForTesting
    synchronized int addNewAsPending(Growth.PollResult<OutputT> pollResult) {
      checkState(
          state.pending.isEmpty(),
          "Should have drained all old pending outputs before adding new, "
              + "but there are %s old pending outputs",
          state.pending.size());
      List<TimestampedValue<OutputT>> newPending = Lists.newArrayList();
      for (TimestampedValue<OutputT> output : pollResult.getOutputs()) {
        OutputT value = output.getValue();
        if (state.completed.containsKey(hash128(value))) {
          continue;
        }
        // TODO: Consider adding only at most N pending elements and ignoring others,
        // instead relying on future poll rounds to provide them, in order to avoid
        // blowing up the state. Combined with garbage collection of GrowthState.completed,
        // this would make the transform scalable to very large poll results.
        newPending.add(TimestampedValue.of(value, output.getTimestamp()));
      }
      if (!newPending.isEmpty()) {
        terminationCondition = terminationCondition.onSeenNewOutput(Instant.now());
      }
      this.pending =
          Lists.newLinkedList(
              Ordering.natural()
                  .onResultOf(
                      new Function<TimestampedValue<OutputT>, Instant>() {
                        @Override
                        public Instant apply(TimestampedValue<OutputT> output) {
                          return output.getTimestamp();
                        }
                      })
                  .sortedCopy(newPending));
      // If poll result doesn't provide a watermark, assume that future new outputs may
      // arrive with about the same timestamps as the current new outputs.
      if (pollResult.getWatermark() != null) {
        this.pollWatermark = pollResult.getWatermark();
      } else if (!pending.isEmpty()) {
        this.pollWatermark = pending.getFirst().getTimestamp();
      }
      if (BoundedWindow.TIMESTAMP_MAX_VALUE.equals(pollWatermark)) {
        isOutputComplete = true;
      }
      return pending.size();
    }

    @VisibleForTesting
    synchronized Instant getWatermark() {
      // Future elements that can be claimed in this restriction come either from
      // "pending" or from future polls, so the total watermark is
      // min(watermark for future polling, earliest remaining pending element)
      return Ordering.natural()
          .nullsLast()
          .min(pollWatermark, pending.isEmpty() ? null : pending.getFirst().getTimestamp());
    }

    @Override
    public synchronized String toString() {
      return "GrowthTracker{"
          + "state="
          + state
          + ", pending=<"
          + pending.size()
          + " elements"
          + (pending.isEmpty() ? "" : (", earliest " + pending.get(0)))
          + ">, claimed=<"
          + claimed.size()
          + " elements>, isOutputComplete="
          + isOutputComplete
          + ", terminationCondition="
          + terminationCondition
          + ", pollWatermark="
          + pollWatermark
          + ", shouldStop="
          + shouldStop
          + '}';
    }
  }

  private static class HashCode128Coder extends AtomicCoder<HashCode> {
    private static final HashCode128Coder INSTANCE = new HashCode128Coder();

    public static HashCode128Coder of() {
      return INSTANCE;
    }

    @Override
    public void encode(HashCode value, OutputStream os) throws IOException {
      checkArgument(
          value.bits() == 128, "Expected a 128-bit hash code, but got %s bits", value.bits());
      byte[] res = new byte[16];
      value.writeBytesTo(res, 0, 16);
      os.write(res);
    }

    @Override
    public HashCode decode(InputStream is) throws IOException {
      byte[] res = new byte[16];
      int numRead = is.read(res, 0, 16);
      checkArgument(numRead == 16, "Expected to read 16 bytes, but read %s", numRead);
      return HashCode.fromBytes(res);
    }
  }

  private static class GrowthStateCoder<OutputT> extends StructuredCoder<GrowthState<OutputT>> {
    public static <OutputT> GrowthStateCoder<OutputT> of(Coder<OutputT> outputCoder) {
      return new GrowthStateCoder<>(outputCoder);
    }

    private static final Coder<Integer> INT_CODER = VarIntCoder.of();
    private static final Coder<Instant> INSTANT_CODER = InstantCoder.of();
    private static final Coder<HashCode> HASH_CODE_CODER = HashCode128Coder.of();

    private final Coder<OutputT> outputCoder;
    private final Coder<Map<HashCode, Instant>> completedCoder;
    private final Coder<List<TimestampedValue<OutputT>>> pendingCoder;
    private final Coder<Growth.TerminationCondition> terminationConditionCoder =
        NullableCoder.of(SerializableCoder.of(Growth.TerminationCondition.class));

    private GrowthStateCoder(Coder<OutputT> outputCoder) {
      this.outputCoder = outputCoder;
      this.completedCoder = MapCoder.of(HASH_CODE_CODER, INSTANT_CODER);
      this.pendingCoder = ListCoder.of(TimestampedValue.TimestampedValueCoder.of(outputCoder));
    }

    @Override
    public void encode(GrowthState<OutputT> value, OutputStream os) throws IOException {
      completedCoder.encode(value.completed, os);
      pendingCoder.encode(value.pending, os);
      INT_CODER.encode(value.isOutputComplete ? 1 : 0, os);
      terminationConditionCoder.encode(value.terminationCondition, os);
      INSTANT_CODER.encode(value.pollWatermark, os);
    }

    @Override
    public GrowthState<OutputT> decode(InputStream is) throws IOException {
      Map<HashCode, Instant> completed = completedCoder.decode(is);
      List<TimestampedValue<OutputT>> pending = pendingCoder.decode(is);
      boolean isOutputComplete = (INT_CODER.decode(is) == 1);
      Growth.TerminationCondition terminationCondition = terminationConditionCoder.decode(is);
      Instant pollWatermark = INSTANT_CODER.decode(is);
      return new GrowthState<>(
          completed, pending, isOutputComplete, terminationCondition, pollWatermark);
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
