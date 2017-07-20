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

import static org.apache.beam.sdk.transforms.Watch.Growth.afterTimeSinceNewOutput;
import static org.apache.beam.sdk.transforms.Watch.Growth.afterTotalOf;
import static org.apache.beam.sdk.transforms.Watch.Growth.allOf;
import static org.apache.beam.sdk.transforms.Watch.Growth.eitherOf;
import static org.apache.beam.sdk.transforms.Watch.Growth.never;
import static org.hamcrest.Matchers.greaterThan;
import static org.joda.time.Duration.standardSeconds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSplittableParDo;
import org.apache.beam.sdk.transforms.Watch.Growth.PollFn;
import org.apache.beam.sdk.transforms.Watch.Growth.PollResult;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.Watch.GrowthState;
import org.apache.beam.sdk.transforms.Watch.GrowthTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Watch}. */
@RunWith(JUnit4.class)
public class WatchTest implements Serializable {
  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  @Category({NeedsRunner.class, UsesSplittableParDo.class})
  public void testSinglePollMultipleInputs() {
    PCollection<KV<String, String>> res =
        p.apply(Create.of("a", "b"))
            .apply(
                Watch.growthOf(
                        new PollFn<String, String>() {
                          @Override
                          public PollResult<String> apply(String input, Instant time) {
                            return PollResult.complete(
                                time, Arrays.asList(input + ".foo", input + ".bar"));
                          }
                        })
                    .withPollInterval(Duration.ZERO));

    PAssert.that(res)
        .containsInAnyOrder(
            Arrays.asList(
                KV.of("a", "a.foo"), KV.of("a", "a.bar"),
                KV.of("b", "b.foo"), KV.of("b", "b.bar")));

    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesSplittableParDo.class})
  public void testMultiplePollsWithTerminationBecauseOutputIsFinal() {
    testMultiplePolls(false);
  }

  @Test
  @Category({NeedsRunner.class, UsesSplittableParDo.class})
  public void testMultiplePollsWithTerminationDueToTerminationCondition() {
    testMultiplePolls(true);
  }

  private void testMultiplePolls(boolean terminationConditionElapsesBeforeOutputIsFinal) {
    List<Integer> all = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    PCollection<Integer> res =
        p.apply(Create.of("a"))
            .apply(
                Watch.growthOf(
                        new TimedPollFn<String, Integer>(
                            all,
                            standardSeconds(1) /* timeToOutputEverything */,
                            standardSeconds(3) /* timeToDeclareOutputFinal */,
                            standardSeconds(30) /* timeToFail */))
                    .withTerminationPerInput(
                        afterTotalOf(
                            standardSeconds(
                                // At 2 seconds, all output has been yielded, but not yet
                                // declared final - so polling should terminate per termination
                                // condition.
                                // At 3 seconds, all output has been yielded (and declared final),
                                // so polling should terminate because of that without waiting for
                                // 100 seconds.
                                terminationConditionElapsesBeforeOutputIsFinal ? 2 : 100)))
                    .withPollInterval(Duration.millis(300))
                    .withOutputCoder(VarIntCoder.of()))
            .apply("Drop input", Values.<Integer>create());

    PAssert.that(res).containsInAnyOrder(all);

    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesSplittableParDo.class})
  public void testMultiplePollsStopAfterTimeSinceNewOutput() {
    List<Integer> all = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    PCollection<Integer> res =
        p.apply(Create.of("a"))
            .apply(
                Watch.growthOf(
                        new TimedPollFn<String, Integer>(
                            all,
                            standardSeconds(1) /* timeToOutputEverything */,
                            // Never declare output final
                            standardSeconds(1000) /* timeToDeclareOutputFinal */,
                            standardSeconds(30) /* timeToFail */))
                    // Should terminate after 4 seconds - earlier than timeToFail
                    .withTerminationPerInput(afterTimeSinceNewOutput(standardSeconds(3)))
                    .withPollInterval(Duration.millis(300))
                    .withOutputCoder(VarIntCoder.of()))
            .apply("Drop input", Values.<Integer>create());

    PAssert.that(res).containsInAnyOrder(all);

    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesSplittableParDo.class})
  public void testSinglePollWithManyResults() {
    // More than the default 100 elements per checkpoint for direct runner.
    final long numResults = 3000;
    PCollection<KV<String, Integer>> res =
        p.apply(Create.of("a"))
            .apply(
                Watch.growthOf(
                        new PollFn<String, KV<String, Integer>>() {
                          @Override
                          public PollResult<KV<String, Integer>> apply(String input, Instant time) {
                            String pollId = UUID.randomUUID().toString();
                            List<KV<String, Integer>> output = Lists.newArrayList();
                            for (int i = 0; i < numResults; ++i) {
                              output.add(KV.of(pollId, i));
                            }
                            return PollResult.complete(time, output);
                          }
                        })
                    .withTerminationPerInput(afterTotalOf(standardSeconds(1)))
                    .withPollInterval(Duration.millis(1))
                    .withOutputCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
            .apply("Drop input", Values.<KV<String, Integer>>create());

    PAssert.that("Poll called only once", res.apply(Keys.<String>create()))
        .satisfies(
            new SerializableFunction<Iterable<String>, Void>() {
              @Override
              public Void apply(Iterable<String> pollIds) {
                assertEquals(1, Sets.newHashSet(pollIds).size());
                return null;
              }
            });
    PAssert.that("Yields all expected results", res.apply("Drop poll id", Values.<Integer>create()))
        .satisfies(
            new SerializableFunction<Iterable<Integer>, Void>() {
              @Override
              public Void apply(Iterable<Integer> input) {
                assertEquals(
                    "Total number of results mismatches",
                    numResults,
                    Lists.newArrayList(input).size());
                assertEquals("Results are not unique", numResults, Sets.newHashSet(input).size());
                return null;
              }
            });

    p.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesSplittableParDo.class})
  public void testMultiplePollsWithManyResults() {
    final long numResults = 3000;
    List<Integer> all = Lists.newArrayList();
    for (int i = 0; i < numResults; ++i) {
      all.add(i);
    }

    PCollection<TimestampedValue<Integer>> res =
        p.apply(Create.of("a"))
            .apply(
                Watch.growthOf(
                        new TimedPollFn<String, Integer>(
                            all,
                            standardSeconds(1) /* timeToOutputEverything */,
                            standardSeconds(3) /* timeToDeclareOutputFinal */,
                            standardSeconds(30) /* timeToFail */))
                    .withPollInterval(Duration.millis(500))
                    .withOutputCoder(VarIntCoder.of()))
            .apply(ReifyTimestamps.<String, Integer>inValues())
            .apply("Drop timestamped input", Values.<TimestampedValue<Integer>>create());

    PAssert.that(res)
        .satisfies(
            new SerializableFunction<Iterable<TimestampedValue<Integer>>, Void>() {
              @Override
              public Void apply(Iterable<TimestampedValue<Integer>> outputs) {
                Function<TimestampedValue<Integer>, Integer> extractValueFn =
                    new Function<TimestampedValue<Integer>, Integer>() {
                      @Nullable
                      @Override
                      public Integer apply(@Nullable TimestampedValue<Integer> input) {
                        return input.getValue();
                      }
                    };
                Function<TimestampedValue<Integer>, Instant> extractTimestampFn =
                    new Function<TimestampedValue<Integer>, Instant>() {
                      @Nullable
                      @Override
                      public Instant apply(@Nullable TimestampedValue<Integer> input) {
                        return input.getTimestamp();
                      }
                    };

                Ordering<TimestampedValue<Integer>> byValue =
                    Ordering.natural().onResultOf(extractValueFn);
                Ordering<TimestampedValue<Integer>> byTimestamp =
                    Ordering.natural().onResultOf(extractTimestampFn);
                // New outputs appear in timestamp order because each output's assigned timestamp
                // is Instant.now() at the time of poll.
                assertTrue(
                    "Outputs must be in timestamp order",
                    byTimestamp.isOrdered(byValue.sortedCopy(outputs)));
                assertEquals(
                    "Yields all expected values",
                    Sets.newHashSet(Iterables.transform(outputs, extractValueFn)).size(),
                    numResults);
                assertThat(
                    "Poll called more than once",
                    Sets.newHashSet(Iterables.transform(outputs, extractTimestampFn)).size(),
                    greaterThan(1));
                return null;
              }
            });

    p.run();
  }

  /**
   * Gradually emits all items from the given list, pairing each one with a UUID that identifies the
   * round of polling, so a client can check how many rounds of polling there were.
   */
  private static class TimedPollFn<InputT, OutputT> implements PollFn<InputT, OutputT> {
    private final Instant baseTime;
    private final List<OutputT> outputs;
    private final Duration timeToOutputEverything;
    private final Duration timeToDeclareOutputFinal;
    private final Duration timeToFail;

    public TimedPollFn(
        List<OutputT> outputs,
        Duration timeToOutputEverything,
        Duration timeToDeclareOutputFinal,
        Duration timeToFail) {
      this.baseTime = Instant.now();
      this.outputs = outputs;
      this.timeToOutputEverything = timeToOutputEverything;
      this.timeToDeclareOutputFinal = timeToDeclareOutputFinal;
      this.timeToFail = timeToFail;
    }

    @Override
    public PollResult<OutputT> apply(InputT input, Instant time) {
      Instant now = Instant.now();
      Duration elapsed = new Duration(baseTime, Instant.now());
      if (elapsed.isLongerThan(timeToFail)) {
        fail(
            String.format(
                "Poll called %s after base time, which is longer than the threshold of %s",
                elapsed, timeToFail));
      }

      double fractionElapsed = 1.0 * elapsed.getMillis() / timeToOutputEverything.getMillis();
      int numToEmit = (int) Math.min(outputs.size(), fractionElapsed * outputs.size());
      List<TimestampedValue<OutputT>> toEmit = Lists.newArrayList();
      for (int i = 0; i < numToEmit; ++i) {
        toEmit.add(TimestampedValue.of(outputs.get(i), now));
      }
      return elapsed.isLongerThan(timeToDeclareOutputFinal)
          ? PollResult.complete(toEmit)
          : PollResult.incomplete(toEmit).withWatermark(now);
    }
  }

  @Test
  public void testTerminationConditionsNever() {
    TerminationCondition c = never().forNewInput(Instant.now());
    assertFalse(c.canStopPolling(Instant.now()));
  }

  @Test
  public void testTerminationConditionsAfterTotalOf() {
    Instant now = Instant.now();
    TerminationCondition c = afterTotalOf(standardSeconds(5)).forNewInput(now);
    assertFalse(c.canStopPolling(now));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(3))));
    assertTrue(c.canStopPolling(now.plus(standardSeconds(6))));
  }

  @Test
  public void testTerminationConditionsAfterTimeSinceNewOutput() {
    Instant now = Instant.now();
    TerminationCondition c = afterTimeSinceNewOutput(standardSeconds(5)).forNewInput(now);
    assertFalse(c.canStopPolling(now));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(3))));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(6))));

    c = c.onSeenNewOutput(now.plus(standardSeconds(3)));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(3))));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(6))));
    assertTrue(c.canStopPolling(now.plus(standardSeconds(9))));

    c = c.onSeenNewOutput(now.plus(standardSeconds(5)));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(3))));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(6))));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(9))));
    assertTrue(c.canStopPolling(now.plus(standardSeconds(11))));
  }

  @Test
  public void testTerminationConditionsEitherOf() {
    Instant now = Instant.now();
    TerminationCondition a = afterTotalOf(standardSeconds(5));
    TerminationCondition b = afterTotalOf(standardSeconds(10));

    TerminationCondition c = eitherOf(a, b).forNewInput(now);
    assertFalse(c.canStopPolling(now.plus(standardSeconds(3))));
    assertTrue(c.canStopPolling(now.plus(standardSeconds(7))));
    assertTrue(c.canStopPolling(now.plus(standardSeconds(12))));
  }

  @Test
  public void testTerminationConditionsAllOf() {
    Instant now = Instant.now();
    TerminationCondition a = afterTotalOf(standardSeconds(5));
    TerminationCondition b = afterTotalOf(standardSeconds(10));

    TerminationCondition c = allOf(a, b).forNewInput(now);
    assertFalse(c.canStopPolling(now.plus(standardSeconds(3))));
    assertFalse(c.canStopPolling(now.plus(standardSeconds(7))));
    assertTrue(c.canStopPolling(now.plus(standardSeconds(12))));
  }

  private static GrowthTracker<String> newTracker(GrowthState<String> state) {
    return new GrowthTracker<>(StringUtf8Coder.of(), state);
  }

  @Test
  public void testGrowthTrackerCheckpointEmpty() {
    // Checkpoint an empty tracker.
    GrowthTracker<String> tracker = newTracker(new GrowthState<String>(never()));
    GrowthState<String> residual = tracker.checkpoint();
    GrowthState<String> primary = tracker.currentRestriction();
    assertEquals(
        primary.toString(),
        new GrowthState<>(
                Collections.<HashCode, Instant>emptyMap() /* completed */,
                Collections.<TimestampedValue<String>>emptyList() /* pending */,
                true /* isOutputFinal */,
                null /* terminationCondition */,
                BoundedWindow.TIMESTAMP_MAX_VALUE /* pollWatermark */)
            .toString());
    assertEquals(
        residual.toString(),
        new GrowthState<>(
                Collections.<HashCode, Instant>emptyMap() /* completed */,
                Collections.<TimestampedValue<String>>emptyList() /* pending */,
                false /* isOutputFinal */,
                never() /* terminationCondition */,
                BoundedWindow.TIMESTAMP_MIN_VALUE /* pollWatermark */)
            .toString());
  }

  @Test
  public void testGrowthTrackerCheckpointNonEmpty() {
    Instant now = Instant.now();
    GrowthTracker<String> tracker = newTracker(new GrowthState<String>(never()));
    tracker.addNewAsPending(
        PollResult.incomplete(
                Arrays.asList(
                    TimestampedValue.of("d", now.plus(standardSeconds(4))),
                    TimestampedValue.of("c", now.plus(standardSeconds(3))),
                    TimestampedValue.of("a", now.plus(standardSeconds(1))),
                    TimestampedValue.of("b", now.plus(standardSeconds(2)))))
            .withWatermark(now.plus(standardSeconds(7))));

    assertEquals(now.plus(standardSeconds(1)), tracker.getWatermark());
    assertTrue(tracker.hasPending());
    assertEquals("a", tracker.tryClaimNextPending().getValue());
    assertTrue(tracker.hasPending());
    assertEquals("b", tracker.tryClaimNextPending().getValue());
    assertTrue(tracker.hasPending());
    assertEquals(now.plus(standardSeconds(3)), tracker.getWatermark());

    GrowthTracker<String> residualTracker = newTracker(tracker.checkpoint());
    GrowthTracker<String> primaryTracker = newTracker(tracker.currentRestriction());

    // Verify primary: should contain what the current tracker claimed, and nothing else.
    assertEquals(now.plus(standardSeconds(1)), primaryTracker.getWatermark());
    assertTrue(primaryTracker.hasPending());
    assertEquals("a", primaryTracker.tryClaimNextPending().getValue());
    assertTrue(primaryTracker.hasPending());
    assertEquals("b", primaryTracker.tryClaimNextPending().getValue());
    assertFalse(primaryTracker.hasPending());
    assertFalse(primaryTracker.shouldPollMore());
    // No more pending elements in primary restriction, and no polling.
    primaryTracker.checkDone();
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE, primaryTracker.getWatermark());

    // Verify residual: should contain what the current tracker didn't claim.
    assertEquals(now.plus(standardSeconds(3)), residualTracker.getWatermark());
    assertTrue(residualTracker.hasPending());
    assertEquals("c", residualTracker.tryClaimNextPending().getValue());
    assertTrue(residualTracker.hasPending());
    assertEquals("d", residualTracker.tryClaimNextPending().getValue());
    assertFalse(residualTracker.hasPending());
    assertTrue(residualTracker.shouldPollMore());
    // No more pending elements in residual restriction, but poll watermark still holds.
    assertEquals(now.plus(standardSeconds(7)), residualTracker.getWatermark());

    // Verify current tracker: it was checkpointed, so should contain nothing else.
    assertNull(tracker.tryClaimNextPending());
    tracker.checkDone();
    assertFalse(tracker.hasPending());
    assertFalse(tracker.shouldPollMore());
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE, tracker.getWatermark());
  }

  @Test
  public void testGrowthTrackerOutputFullyBeforeCheckpointIncomplete() {
    Instant now = Instant.now();
    GrowthTracker<String> tracker = newTracker(new GrowthState<String>(never()));
    tracker.addNewAsPending(
        PollResult.incomplete(
                Arrays.asList(
                    TimestampedValue.of("d", now.plus(standardSeconds(4))),
                    TimestampedValue.of("c", now.plus(standardSeconds(3))),
                    TimestampedValue.of("a", now.plus(standardSeconds(1))),
                    TimestampedValue.of("b", now.plus(standardSeconds(2)))))
            .withWatermark(now.plus(standardSeconds(7))));

    assertEquals("a", tracker.tryClaimNextPending().getValue());
    assertEquals("b", tracker.tryClaimNextPending().getValue());
    assertEquals("c", tracker.tryClaimNextPending().getValue());
    assertEquals("d", tracker.tryClaimNextPending().getValue());
    assertFalse(tracker.hasPending());
    assertEquals(now.plus(standardSeconds(7)), tracker.getWatermark());

    GrowthTracker<String> residualTracker = newTracker(tracker.checkpoint());
    GrowthTracker<String> primaryTracker = newTracker(tracker.currentRestriction());

    // Verify primary: should contain what the current tracker claimed, and nothing else.
    assertEquals(now.plus(standardSeconds(1)), primaryTracker.getWatermark());
    assertTrue(primaryTracker.hasPending());
    assertEquals("a", primaryTracker.tryClaimNextPending().getValue());
    assertTrue(primaryTracker.hasPending());
    assertEquals("b", primaryTracker.tryClaimNextPending().getValue());
    assertTrue(primaryTracker.hasPending());
    assertEquals("c", primaryTracker.tryClaimNextPending().getValue());
    assertTrue(primaryTracker.hasPending());
    assertEquals("d", primaryTracker.tryClaimNextPending().getValue());
    assertFalse(primaryTracker.hasPending());
    assertFalse(primaryTracker.shouldPollMore());
    // No more pending elements in primary restriction, and no polling.
    primaryTracker.checkDone();
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE, primaryTracker.getWatermark());

    // Verify residual: should contain what the current tracker didn't claim.
    assertFalse(residualTracker.hasPending());
    assertTrue(residualTracker.shouldPollMore());
    // No more pending elements in residual restriction, but poll watermark still holds.
    assertEquals(now.plus(standardSeconds(7)), residualTracker.getWatermark());

    // Verify current tracker: it was checkpointed, so should contain nothing else.
    tracker.checkDone();
    assertFalse(tracker.hasPending());
    assertFalse(tracker.shouldPollMore());
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE, tracker.getWatermark());
  }

  @Test
  public void testGrowthTrackerPollAfterCheckpointIncompleteWithNewOutputs() {
    Instant now = Instant.now();
    GrowthTracker<String> tracker = newTracker(new GrowthState<String>(never()));
    tracker.addNewAsPending(
        PollResult.incomplete(
                Arrays.asList(
                    TimestampedValue.of("d", now.plus(standardSeconds(4))),
                    TimestampedValue.of("c", now.plus(standardSeconds(3))),
                    TimestampedValue.of("a", now.plus(standardSeconds(1))),
                    TimestampedValue.of("b", now.plus(standardSeconds(2)))))
            .withWatermark(now.plus(standardSeconds(7))));

    assertEquals("a", tracker.tryClaimNextPending().getValue());
    assertEquals("b", tracker.tryClaimNextPending().getValue());
    assertEquals("c", tracker.tryClaimNextPending().getValue());
    assertEquals("d", tracker.tryClaimNextPending().getValue());

    GrowthState<String> checkpoint = tracker.checkpoint();
    // Simulate resuming from the checkpoint and adding more elements.
    {
      GrowthTracker<String> residualTracker = newTracker(checkpoint);
      residualTracker.addNewAsPending(
          PollResult.incomplete(
                  Arrays.asList(
                      TimestampedValue.of("e", now.plus(standardSeconds(5))),
                      TimestampedValue.of("d", now.plus(standardSeconds(4))),
                      TimestampedValue.of("c", now.plus(standardSeconds(3))),
                      TimestampedValue.of("a", now.plus(standardSeconds(1))),
                      TimestampedValue.of("b", now.plus(standardSeconds(2))),
                      TimestampedValue.of("f", now.plus(standardSeconds(8)))))
              .withWatermark(now.plus(standardSeconds(12))));

      assertEquals(now.plus(standardSeconds(5)), residualTracker.getWatermark());
      assertEquals("e", residualTracker.tryClaimNextPending().getValue());
      assertEquals(now.plus(standardSeconds(8)), residualTracker.getWatermark());
      assertEquals("f", residualTracker.tryClaimNextPending().getValue());

      assertFalse(residualTracker.hasPending());
      assertTrue(residualTracker.shouldPollMore());
      assertEquals(now.plus(standardSeconds(12)), residualTracker.getWatermark());
    }
    // Try same without an explicitly specified watermark.
    {
      GrowthTracker<String> residualTracker = newTracker(checkpoint);
      residualTracker.addNewAsPending(
          PollResult.incomplete(
              Arrays.asList(
                  TimestampedValue.of("e", now.plus(standardSeconds(5))),
                  TimestampedValue.of("d", now.plus(standardSeconds(4))),
                  TimestampedValue.of("c", now.plus(standardSeconds(3))),
                  TimestampedValue.of("a", now.plus(standardSeconds(1))),
                  TimestampedValue.of("b", now.plus(standardSeconds(2))),
                  TimestampedValue.of("f", now.plus(standardSeconds(8))))));

      assertEquals(now.plus(standardSeconds(5)), residualTracker.getWatermark());
      assertEquals("e", residualTracker.tryClaimNextPending().getValue());
      assertEquals(now.plus(standardSeconds(5)), residualTracker.getWatermark());
      assertEquals("f", residualTracker.tryClaimNextPending().getValue());

      assertFalse(residualTracker.hasPending());
      assertTrue(residualTracker.shouldPollMore());
      assertEquals(now.plus(standardSeconds(5)), residualTracker.getWatermark());
    }
  }

  @Test
  public void testGrowthTrackerPollAfterCheckpointWithoutNewOutputs() {
    Instant now = Instant.now();
    GrowthTracker<String> tracker = newTracker(new GrowthState<String>(never()));
    tracker.addNewAsPending(
        PollResult.incomplete(
                Arrays.asList(
                    TimestampedValue.of("d", now.plus(standardSeconds(4))),
                    TimestampedValue.of("c", now.plus(standardSeconds(3))),
                    TimestampedValue.of("a", now.plus(standardSeconds(1))),
                    TimestampedValue.of("b", now.plus(standardSeconds(2)))))
            .withWatermark(now.plus(standardSeconds(7))));

    assertEquals("a", tracker.tryClaimNextPending().getValue());
    assertEquals("b", tracker.tryClaimNextPending().getValue());
    assertEquals("c", tracker.tryClaimNextPending().getValue());
    assertEquals("d", tracker.tryClaimNextPending().getValue());

    // Simulate resuming from the checkpoint but there are no new elements.
    GrowthState<String> checkpoint = tracker.checkpoint();
    {
      GrowthTracker<String> residualTracker = newTracker(checkpoint);
      residualTracker.addNewAsPending(
          PollResult.incomplete(
                  Arrays.asList(
                      TimestampedValue.of("c", now.plus(standardSeconds(3))),
                      TimestampedValue.of("d", now.plus(standardSeconds(4))),
                      TimestampedValue.of("a", now.plus(standardSeconds(1))),
                      TimestampedValue.of("b", now.plus(standardSeconds(2)))))
              .withWatermark(now.plus(standardSeconds(12))));

      assertFalse(residualTracker.hasPending());
      assertTrue(residualTracker.shouldPollMore());
      assertEquals(now.plus(standardSeconds(12)), residualTracker.getWatermark());
    }
    // Try the same without an explicitly specified watermark
    {
      GrowthTracker<String> residualTracker = newTracker(checkpoint);
      residualTracker.addNewAsPending(
          PollResult.incomplete(
              Arrays.asList(
                  TimestampedValue.of("c", now.plus(standardSeconds(3))),
                  TimestampedValue.of("d", now.plus(standardSeconds(4))),
                  TimestampedValue.of("a", now.plus(standardSeconds(1))),
                  TimestampedValue.of("b", now.plus(standardSeconds(2))))));
      // No new elements and no explicit watermark supplied - should reuse old watermark.
      assertEquals(now.plus(standardSeconds(7)), residualTracker.getWatermark());
    }
  }

  @Test
  public void testGrowthTrackerPollAfterCheckpointWithoutNewOutputsNoWatermark() {
    Instant now = Instant.now();
    GrowthTracker<String> tracker = newTracker(new GrowthState<String>(never()));
    tracker.addNewAsPending(
        PollResult.incomplete(
            Arrays.asList(
                TimestampedValue.of("d", now.plus(standardSeconds(4))),
                TimestampedValue.of("c", now.plus(standardSeconds(3))),
                TimestampedValue.of("a", now.plus(standardSeconds(1))),
                TimestampedValue.of("b", now.plus(standardSeconds(2))))));
    assertEquals("a", tracker.tryClaimNextPending().getValue());
    assertEquals("b", tracker.tryClaimNextPending().getValue());
    assertEquals("c", tracker.tryClaimNextPending().getValue());
    assertEquals("d", tracker.tryClaimNextPending().getValue());
    assertEquals(now.plus(standardSeconds(1)), tracker.getWatermark());

    // Simulate resuming from the checkpoint but there are no new elements.
    GrowthState<String> checkpoint = tracker.checkpoint();
    GrowthTracker<String> residualTracker = newTracker(checkpoint);
    residualTracker.addNewAsPending(
        PollResult.incomplete(
            Arrays.asList(
                TimestampedValue.of("c", now.plus(standardSeconds(3))),
                TimestampedValue.of("d", now.plus(standardSeconds(4))),
                TimestampedValue.of("a", now.plus(standardSeconds(1))),
                TimestampedValue.of("b", now.plus(standardSeconds(2))))));
    // No new elements and no explicit watermark supplied - should keep old watermark.
    assertEquals(now.plus(standardSeconds(1)), residualTracker.getWatermark());
  }

  @Test
  public void testGrowthTrackerRepeatedEmptyPollWatermark() {
    // Empty poll result with no watermark
    {
      GrowthTracker<String> tracker = newTracker(new GrowthState<String>(never()));
      tracker.addNewAsPending(
          PollResult.incomplete(Collections.<TimestampedValue<String>>emptyList()));
      assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE, tracker.getWatermark());

      // Simulate resuming from the checkpoint but there are still no new elements.
      GrowthTracker<String> residualTracker = newTracker(tracker.checkpoint());
      tracker.addNewAsPending(
          PollResult.incomplete(Collections.<TimestampedValue<String>>emptyList()));
      // No new elements and no explicit watermark supplied - still no watermark.
      assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE, residualTracker.getWatermark());
    }
    // Empty poll result with watermark
    {
      Instant now = Instant.now();
      GrowthTracker<String> tracker = newTracker(new GrowthState<String>(never()));
      tracker.addNewAsPending(
          PollResult.incomplete(Collections.<TimestampedValue<String>>emptyList())
              .withWatermark(now));
      assertEquals(now, tracker.getWatermark());

      // Simulate resuming from the checkpoint but there are still no new elements.
      GrowthTracker<String> residualTracker = newTracker(tracker.checkpoint());
      tracker.addNewAsPending(
          PollResult.incomplete(Collections.<TimestampedValue<String>>emptyList()));
      // No new elements and no explicit watermark supplied - should keep old watermark.
      assertEquals(now, residualTracker.getWatermark());
    }
  }

  @Test
  public void testGrowthTrackerOutputFullyBeforeCheckpointComplete() {
    Instant now = Instant.now();
    GrowthTracker<String> tracker = newTracker(new GrowthState<String>(never()));
    tracker.addNewAsPending(
        PollResult.complete(
            Arrays.asList(
                TimestampedValue.of("d", now.plus(standardSeconds(4))),
                TimestampedValue.of("c", now.plus(standardSeconds(3))),
                TimestampedValue.of("a", now.plus(standardSeconds(1))),
                TimestampedValue.of("b", now.plus(standardSeconds(2))))));

    assertEquals("a", tracker.tryClaimNextPending().getValue());
    assertEquals("b", tracker.tryClaimNextPending().getValue());
    assertEquals("c", tracker.tryClaimNextPending().getValue());
    assertEquals("d", tracker.tryClaimNextPending().getValue());
    assertFalse(tracker.hasPending());
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE, tracker.getWatermark());

    GrowthTracker<String> residualTracker = newTracker(tracker.checkpoint());

    // Verify residual: should be empty, since output was final.
    residualTracker.checkDone();
    assertFalse(residualTracker.hasPending());
    assertFalse(residualTracker.shouldPollMore());
    // No more pending elements in residual restriction, but poll watermark still holds.
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE, residualTracker.getWatermark());

    // Verify current tracker: it was checkpointed, so should contain nothing else.
    tracker.checkDone();
    assertFalse(tracker.hasPending());
    assertFalse(tracker.shouldPollMore());
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE, tracker.getWatermark());
  }
}
