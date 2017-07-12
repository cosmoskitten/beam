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

import static org.apache.beam.sdk.transforms.Watch.Growth.afterTotalOf;
import static org.apache.beam.sdk.transforms.Watch.Growth.outputCanGrow;
import static org.apache.beam.sdk.transforms.Watch.Growth.outputIsFinal;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Watch.Growth.PollFn;
import org.apache.beam.sdk.transforms.Watch.Growth.PollResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WatchTest implements Serializable {
  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testSinglePoll() {
    PCollection<KV<String, String>> res =
        p.apply(Create.of("a"))
            .apply(
                Watch.growthOf(
                        new PollFn<String, String>() {
                          @Override
                          public PollResult<String> apply(TimestampedValue<String> input) {
                            Instant time = input.getTimestamp();
                            return outputIsFinal(
                                Arrays.asList(
                                    TimestampedValue.of(input.getValue() + ".foo", time),
                                    TimestampedValue.of(input.getValue() + ".bar", time)));
                          }
                        })
                    .withPollInterval(Duration.ZERO)
                    .withOutputCoder(StringUtf8Coder.of()));

    PAssert.that(res).containsInAnyOrder(Arrays.asList(KV.of("a", "a.foo"), KV.of("a", "a.bar")));

    p.run();
  }

  @Test
  public void testMultiplePolls() {
    PCollection<KV<String, String>> res =
        p.apply(Create.of("a"))
            .apply(
                Watch.growthOf(
                        new PollFn<String, String>() {
                          @Override
                          public PollResult<String> apply(TimestampedValue<String> input) {
                            Instant time = input.getTimestamp();
                            return outputCanGrow(
                                Arrays.asList(
                                    TimestampedValue.of(input.getValue() + ".foo", time),
                                    TimestampedValue.of(input.getValue() + ".bar", time)),
                                time);
                          }
                        })
                    .withTerminationPerInput(afterTotalOf(Duration.standardSeconds(1)))
                    .withPollInterval(Duration.millis(1))
                    .withOutputCoder(StringUtf8Coder.of()));

    PAssert.that(res).containsInAnyOrder(Arrays.asList(KV.of("a", "a.foo"), KV.of("a", "a.bar")));

    p.run();
  }

  /*
   * Things to test:
   * - Poll returns more elements than fits in a checkpoint, once.
   * - Poll returns more elements than fits in a checkpoint, and next poll returns even more.
   * - Polling terminates due to termination condition while output is non-final
   * - Test termination conditions themselves
   * - Test tracker (that checkpoints add up)
   * - Test watermark
   *
   */
}
