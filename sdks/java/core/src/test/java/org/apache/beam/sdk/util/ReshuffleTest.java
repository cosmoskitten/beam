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
package org.apache.beam.sdk.util;

import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link Reshuffle}.
 */
@RunWith(JUnit4.class)
public class ReshuffleTest implements Serializable {

  private static final List<KV<String, Integer>> ARBITRARY_KVS = ImmutableList.of(
        KV.of("k1", 3),
        KV.of("k5", Integer.MAX_VALUE),
        KV.of("k5", Integer.MIN_VALUE),
        KV.of("k2", 66),
        KV.of("k1", 4),
        KV.of("k2", -33),
        KV.of("k3", 0));

  // TODO: test with more than one value per key
  private static final List<KV<String, Integer>> GBK_TESTABLE_KVS = ImmutableList.of(
        KV.of("k1", 3),
        KV.of("k2", 4));

  private static final List<KV<String, Iterable<Integer>>> GROUPED_TESTABLE_KVS = ImmutableList.of(
        KV.of("k1", (Iterable<Integer>) ImmutableList.of(3)),
        KV.of("k2", (Iterable<Integer>) ImmutableList.of(4)));

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(RunnableOnService.class)
  public void testJustReshuffle() {

    PCollection<KV<String, Integer>> input = pipeline
        .apply(Create.of(ARBITRARY_KVS)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));

    PCollection<KV<String, Integer>> output = input
        .apply(Reshuffle.<String, Integer>of());

    PAssert.that(output).containsInAnyOrder(ARBITRARY_KVS);

    assertEquals(
        input.getWindowingStrategy(),
        output.getWindowingStrategy());

    pipeline.run();
  }

  /**
   * Tests that timestamps are not moved forwards in time after applying a {@link Reshuffle} with
   * the default {@link WindowingStrategy}.
   */
  @Test
  @Category(RunnableOnService.class)
  public void testReshuffleDoesNotMoveTimestampsForwards() {
    PCollection<KV<String, Instant>> input =
        pipeline
            .apply(
                Create.timestamped(
                        TimestampedValue.of("foo", BoundedWindow.TIMESTAMP_MIN_VALUE),
                        TimestampedValue.of("foo", new Instant(0)),
                        TimestampedValue.of("bar", new Instant(33)),
                        TimestampedValue.of("bar", BoundedWindow.TIMESTAMP_MAX_VALUE))
                    .withCoder(StringUtf8Coder.of()))
            .apply(ParDo.of(new PairWithTimestampFn<String>()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), InstantCoder.of()));

    PCollection<KV<KV<String, Instant>, Instant>> output =
        input
            .apply(Reshuffle.<String, Instant>of())
            .apply(ParDo.of(new PairWithTimestampFn<KV<String, Instant>>()));

    PAssert.that(output)
        .satisfies(
            new SerializableFunction<Iterable<KV<KV<String, Instant>, Instant>>, Void>() {
              @Override
              public Void apply(Iterable<KV<KV<String, Instant>, Instant>> input) {
                for (KV<KV<String, Instant>, Instant> elem : input) {
                  Instant originalTimestamp = elem.getKey().getValue();
                  Instant afterReshuffleTimestamp = elem.getValue();
                  assertThat(
                      afterReshuffleTimestamp,
                      not(Matchers.<ReadableInstant>greaterThan(originalTimestamp)));
                }
                return null;
              }
            });

    pipeline.run();
  }

  private static class PairWithTimestampFn<T> extends DoFn<T, KV<T, Instant>> {
    @ProcessElement
    public void pairWithTimestamp(ProcessContext ctxt) {
      ctxt.output(KV.of(ctxt.element(), ctxt.timestamp()));
    }
  };

  @Test
  @Category(RunnableOnService.class)
  public void testReshuffleAfterSessionsAndGroupByKey() {

    PCollection<KV<String, Iterable<Integer>>> input = pipeline
        .apply(Create.of(GBK_TESTABLE_KVS)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
        .apply(Window.<KV<String, Integer>>into(
            Sessions.withGapDuration(Duration.standardMinutes(10))))
        .apply(GroupByKey.<String, Integer>create());

    PCollection<KV<String, Iterable<Integer>>> output = input
        .apply(Reshuffle.<String, Iterable<Integer>>of());

    PAssert.that(output).containsInAnyOrder(GROUPED_TESTABLE_KVS);

    assertEquals(
        input.getWindowingStrategy(),
        output.getWindowingStrategy());

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testReshuffleAfterFixedWindowsAndGroupByKey() {

    PCollection<KV<String, Iterable<Integer>>> input = pipeline
        .apply(Create.of(GBK_TESTABLE_KVS)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
        .apply(Window.<KV<String, Integer>>into(
            FixedWindows.of(Duration.standardMinutes(10L))))
        .apply(GroupByKey.<String, Integer>create());

    PCollection<KV<String, Iterable<Integer>>> output = input
        .apply(Reshuffle.<String, Iterable<Integer>>of());

    PAssert.that(output).containsInAnyOrder(GROUPED_TESTABLE_KVS);

    assertEquals(
        input.getWindowingStrategy(),
        output.getWindowingStrategy());

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testReshuffleAfterSlidingWindowsAndGroupByKey() {

    PCollection<KV<String, Iterable<Integer>>> input = pipeline
        .apply(Create.of(GBK_TESTABLE_KVS)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
        .apply(Window.<KV<String, Integer>>into(
            FixedWindows.of(Duration.standardMinutes(10L))))
        .apply(GroupByKey.<String, Integer>create());

    PCollection<KV<String, Iterable<Integer>>> output = input
        .apply(Reshuffle.<String, Iterable<Integer>>of());

    PAssert.that(output).containsInAnyOrder(GROUPED_TESTABLE_KVS);

    assertEquals(
        input.getWindowingStrategy(),
        output.getWindowingStrategy());

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testReshuffleAfterFixedWindows() {

    PCollection<KV<String, Integer>> input = pipeline
        .apply(Create.of(ARBITRARY_KVS)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
        .apply(Window.<KV<String, Integer>>into(
            FixedWindows.of(Duration.standardMinutes(10L))));

    PCollection<KV<String, Integer>> output = input
        .apply(Reshuffle.<String, Integer>of());

    PAssert.that(output).containsInAnyOrder(ARBITRARY_KVS);

    assertEquals(
        input.getWindowingStrategy(),
        output.getWindowingStrategy());

    pipeline.run();
  }


  @Test
  @Category(RunnableOnService.class)
  public void testReshuffleAfterSlidingWindows() {

    PCollection<KV<String, Integer>> input = pipeline
        .apply(Create.of(ARBITRARY_KVS)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
        .apply(Window.<KV<String, Integer>>into(
            FixedWindows.of(Duration.standardMinutes(10L))));

    PCollection<KV<String, Integer>> output = input
        .apply(Reshuffle.<String, Integer>of());

    PAssert.that(output).containsInAnyOrder(ARBITRARY_KVS);

    assertEquals(
        input.getWindowingStrategy(),
        output.getWindowingStrategy());

    pipeline.run();
  }
}
