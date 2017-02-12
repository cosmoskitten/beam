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
package org.apache.beam.sdk.transforms.windowing;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for utilities in {@link Triggers}. */
@RunWith(Parameterized.class)
public class TriggersTest {

  @AutoValue
  abstract static class ToProtoAndBackSpec {
    abstract Trigger getTrigger();
  }

  private static ToProtoAndBackSpec toProtoAndBack(Trigger trigger) {
    return new AutoValue_TriggersTest_ToProtoAndBackSpec(trigger);
  }

  @Parameters(name = "{index}: {0}")
  public static Iterable<Object[]> data() {
    return ImmutableList.copyOf(
        new Object[][] {

          // Atomic triggers
          {toProtoAndBack(AfterWatermark.pastEndOfWindow())},
          {toProtoAndBack(AfterPane.elementCountAtLeast(73))},
          {toProtoAndBack(new AfterSynchronizedProcessingTime())},
          {toProtoAndBack(Never.ever())},
          {toProtoAndBack(DefaultTrigger.of())},
          {toProtoAndBack(AfterProcessingTime.pastFirstElementInPane())},
          {
            toProtoAndBack(
                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(23)))
          },
          {
            toProtoAndBack(
                AfterProcessingTime.pastFirstElementInPane()
                    .alignedTo(Duration.millis(5), new Instant(27)))
          },
          {
            toProtoAndBack(
                AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(Duration.standardSeconds(3))
                    .alignedTo(Duration.millis(5), new Instant(27))
                    .plusDelayOf(Duration.millis(13)))
          },

          // Composite triggers
          {
            toProtoAndBack(
                AfterAll.of(AfterPane.elementCountAtLeast(79), AfterWatermark.pastEndOfWindow()))
          },
          {
            toProtoAndBack(
                AfterEach.inOrder(
                    AfterPane.elementCountAtLeast(79), AfterPane.elementCountAtLeast(3)))
          },
          {
            toProtoAndBack(
                AfterFirst.of(AfterWatermark.pastEndOfWindow(), AfterPane.elementCountAtLeast(3)))
          },
          {
            toProtoAndBack(
                AfterWatermark.pastEndOfWindow().withEarlyFirings(AfterPane.elementCountAtLeast(3)))
          },
          {
            toProtoAndBack(
                AfterWatermark.pastEndOfWindow().withLateFirings(AfterPane.elementCountAtLeast(3)))
          },
          {
            toProtoAndBack(
                AfterWatermark.pastEndOfWindow()
                    .withEarlyFirings(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.millis(42)))
                    .withLateFirings(AfterPane.elementCountAtLeast(3)))
          },
          {toProtoAndBack(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))},
          {
            toProtoAndBack(
                Repeatedly.forever(AfterPane.elementCountAtLeast(1))
                    .orFinally(AfterWatermark.pastEndOfWindow()))
          }
        });
  }

  @Parameter(0)
  public ToProtoAndBackSpec toProtoAndBackSpec;

  @Test
  public void testToProtoAndBack() throws Exception {
    Trigger trigger = toProtoAndBackSpec.getTrigger();
    Trigger toProtoAndBackTrigger = Triggers.protoToTrigger(Triggers.triggerToProto(trigger));

    assertThat(toProtoAndBackTrigger, equalTo(trigger));
  }
}
