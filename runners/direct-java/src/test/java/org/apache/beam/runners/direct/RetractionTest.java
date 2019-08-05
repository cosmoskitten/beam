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

import static org.apache.beam.sdk.transforms.windowing.AfterWatermark.pastEndOfWindow;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

/** RetractionTest. */
public class RetractionTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  private static final Duration WINDOW_LENGTH = Duration.standardMinutes(2);
  private static final Duration LATENESS_HORIZON = Duration.standardDays(1);

  @Test
  public void retractionSimpleTest() {
    Instant baseTime = new Instant(0L);
    Duration oneMin = Duration.standardMinutes(1);

    TestStream<String> events =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(baseTime)

            // First batch of element
            .addElements(
                TimestampedValue.of("Java", baseTime.plus(oneMin)),
                TimestampedValue.of("Java", baseTime.plus(oneMin)),
                TimestampedValue.of("Python", baseTime.plus(oneMin)),
                TimestampedValue.of("Go", baseTime.plus(oneMin)))
            .advanceWatermarkTo(baseTime.plus(WINDOW_LENGTH).plus(oneMin))
            .addElements(TimestampedValue.of("Java", baseTime.plus(oneMin)))

            // Fire all
            .advanceWatermarkToInfinity();

    PCollection<KV<String, Long>> pc =
        pipeline
            .apply(events)
            .apply(
                "window",
                Window.<String>into(FixedWindows.of(WINDOW_LENGTH))
                    .triggering(
                        pastEndOfWindow()
                            .withLateFirings(AfterProcessingTime.pastFirstElementInPane()))
                    .withAllowedLateness(LATENESS_HORIZON)
                    .retractingFiredPanes())
            .apply(Count.perElement());

    IntervalWindow window = new IntervalWindow(baseTime, WINDOW_LENGTH);

    PAssert.that(pc)
        .filterAdditions()
        .inOnTimePane(window)
        .containsInAnyOrder(KV.of("Java", 2L), KV.of("Python", 1L), KV.of("Go", 1L));

    PAssert.that(pc).filterAdditions().inLatePane(window).containsInAnyOrder(KV.of("Java", 3L));

    pipeline.run();
  }

  // @Test
  // public void retractionSimpleTest2() {
  //   PCollection<KV<String, Long>> pc =
  //       pipeline.apply(Create.of("Java", "Java", "Python", "Go")).apply(Count.perElement());
  //
  //   PAssert.that(pc).filterRetractions().containsInAnyOrder();
  //
  //   pipeline.run();
  // }
}
