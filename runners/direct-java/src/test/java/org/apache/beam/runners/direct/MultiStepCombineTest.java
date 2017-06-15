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

package org.apache.beam.runners.direct;

import java.io.Serializable;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link MultiStepCombine}.
 */
@RunWith(JUnit4.class)
public class MultiStepCombineTest implements Serializable {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private transient KvCoder<String, Long> combinedCoder =
      KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of());

  @Test
  public void testMultiStepCombine() {
    PCollection<KV<String, Long>> combined =
        pipeline
            .apply(
                Create.of(
                    KV.of("foo", 1L),
                    KV.of("bar", 2L),
                    KV.of("bizzle", 3L),
                    KV.of("bar", 4L),
                    KV.of("bizzle", 11L)))
            .apply(MultiStepCombine.<String, Long, long[], Long>of(Sum.ofLongs(), combinedCoder));

    PAssert.that(combined)
        .containsInAnyOrder(KV.of("foo", 1L), KV.of("bar", 6L), KV.of("bizzle", 14L));
    pipeline.run();
  }

  @Test
  public void testMultiStepCombineWindowed() {
    SlidingWindows windowFn = SlidingWindows.of(Duration.millis(6L)).every(Duration.millis(3L));
    PCollection<KV<String, Long>> combined =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of("foo", 1L), new Instant(1L)),
                    TimestampedValue.of(KV.of("bar", 2L), new Instant(2L)),
                    TimestampedValue.of(KV.of("bizzle", 3L), new Instant(3L)),
                    TimestampedValue.of(KV.of("bar", 4L), new Instant(4L)),
                    TimestampedValue.of(KV.of("bizzle", 11L), new Instant(11L))))
            .apply(Window.<KV<String, Long>>into(windowFn))
            .apply(MultiStepCombine.<String, Long, long[], Long>of(Sum.ofLongs(), combinedCoder));

    PAssert.that("Windows should combine only elements in their windows", combined)
        .inWindow(new IntervalWindow(new Instant(0L), Duration.millis(6L)))
        .containsInAnyOrder(KV.of("foo", 1L), KV.of("bar", 6L), KV.of("bizzle", 3L));
    PAssert.that("Elements should appear in all the windows they are assigned to", combined)
        .inWindow(new IntervalWindow(new Instant(-3L), Duration.millis(6L)))
        .containsInAnyOrder(KV.of("foo", 1L), KV.of("bar", 2L));
    PAssert.that(combined)
        .inWindow(new IntervalWindow(new Instant(6L), Duration.millis(6L)))
        .containsInAnyOrder(KV.of("bizzle", 11L));
    PAssert.that(combined)
        .containsInAnyOrder(
            KV.of("foo", 1L),
            KV.of("foo", 1L),
            KV.of("bar", 6L),
            KV.of("bar", 2L),
            KV.of("bar", 4L),
            KV.of("bizzle", 11L),
            KV.of("bizzle", 11L),
            KV.of("bizzle", 3L),
            KV.of("bizzle", 3L));
    pipeline.run();
  }

  @Test
  public void testMultiStepCombineMergingWindows() {
    Sessions mergingWindows = Sessions.withGapDuration(Duration.millis(5));
    PCollection<KV<String, Long>> combined =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of("foo", 1L), new Instant(1L)),
                    TimestampedValue.of(KV.of("foo", 4L), new Instant(4L)),
                    TimestampedValue.of(KV.of("bazzle", 4L), new Instant(4L)),
                    TimestampedValue.of(KV.of("foo", 12L), new Instant(12L))))
            .apply(Window.<KV<String, Long>>into(mergingWindows))
            .apply(MultiStepCombine.<String, Long, long[], Long>of(Sum.ofLongs(), combinedCoder));
    PAssert.that(combined)
        .inWindow(new IntervalWindow(new Instant(1L), new Instant(9L)))
        .containsInAnyOrder(KV.of("foo", 5L));
    PAssert.that(combined)
        .inWindow(new IntervalWindow(new Instant(12L), new Instant(17L)))
        .containsInAnyOrder(KV.of("foo", 12L));
    PAssert.that(combined)
        .inWindow(new IntervalWindow(new Instant(4L), new Instant(9L)))
        .containsInAnyOrder(KV.of("bazzle", 4L));
    PAssert.that(combined)
        .containsInAnyOrder(KV.of("foo", 5L), KV.of("bazzle", 4L), KV.of("foo", 12L));
    pipeline.run();
  }

  @Test
  public void testMultiStepCombineTimestampCombiner() {
    TimestampCombiner combiner = TimestampCombiner.LATEST;
    combinedCoder = KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of());
    PCollection<KV<String, Long>> combined =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of("foo", 4L), new Instant(1L)),
                    TimestampedValue.of(KV.of("foo", 1L), new Instant(4L)),
                    TimestampedValue.of(KV.of("bazzle", 4L), new Instant(4L)),
                    TimestampedValue.of(KV.of("foo", 12L), new Instant(12L))))
            .apply(
                Window.<KV<String, Long>>into(FixedWindows.of(Duration.millis(5L)))
                    .withTimestampCombiner(combiner))
            .apply(MultiStepCombine.<String, Long, long[], Long>of(Max.ofLongs(), combinedCoder));
    PCollection<KV<String, TimestampedValue<Long>>> reified =
        combined.apply(
            ParDo.of(
                new DoFn<KV<String, Long>, KV<String, TimestampedValue<Long>>>() {
                  @ProcessElement
                  public void reifyTimestamp(ProcessContext context) {
                    context.output(
                        KV.of(
                            context.element().getKey(),
                            TimestampedValue.of(
                                context.element().getValue(), context.timestamp())));
                  }
                }));

    PAssert.that(reified)
        .containsInAnyOrder(
            KV.of("foo", TimestampedValue.of(4L, new Instant(4L))),
            KV.of("bazzle", TimestampedValue.of(4L, new Instant(4L))),
            KV.of("foo", TimestampedValue.of(12L, new Instant(12L))));
    pipeline.run();
  }
}
