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
package org.apache.beam.runners.core;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Tests for {@link SplittableParDo}. */
public class SplittableParDoTest {
  @DefaultCoder(SerializableCoder.class)
  private static class SomeRestriction implements Serializable {}

  private abstract static class SomeRestrictionTracker
      implements RestrictionTracker<SomeRestriction> {}

  private static class BoundedFakeFn extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(ProcessContext context, SomeRestrictionTracker tracker) {}

    @GetInitialRestriction
    public SomeRestriction getInitialRestriction(Integer element) {
      return null;
    }

    @NewTracker
    public SomeRestrictionTracker newTracker(SomeRestriction restriction) {
      return null;
    }
  }

  private static class UnboundedFakeFn extends DoFn<Integer, String> {
    @ProcessElement
    public ProcessContinuation processElement(
        ProcessContext context, SomeRestrictionTracker tracker) {
      return ProcessContinuation.stop();
    }

    @GetInitialRestriction
    public SomeRestriction getInitialRestriction(Integer element) {
      return null;
    }

    @NewTracker
    public SomeRestrictionTracker newTracker(SomeRestriction restriction) {
      return null;
    }
  }

  @Test
  @Category(RunnableOnService.class)
  public void testBoundedness() {
    DoFn<Integer, String> boundedFn = new BoundedFakeFn();
    DoFn<Integer, String> unboundedFn = new UnboundedFakeFn();

    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> boundedPC =
        pipeline
            .apply("bounded", Create.of(1, 2, 3))
            .setIsBoundedInternal(PCollection.IsBounded.BOUNDED);
    PCollection<Integer> unboundedPC =
        pipeline
            .apply("unbounded", Create.of(1, 2, 3))
            .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED);

    assertEquals(
        PCollection.IsBounded.BOUNDED,
        boundedPC.apply(new SplittableParDo<>("bounded to bounded", boundedFn)).isBounded());
    assertEquals(
        PCollection.IsBounded.BOUNDED,
        unboundedPC.apply(new SplittableParDo<>("bounded to unbounded", boundedFn)).isBounded());
    assertEquals(
        PCollection.IsBounded.BOUNDED,
        boundedPC.apply(new SplittableParDo<>("unbounded to bounded", unboundedFn)).isBounded());
    assertEquals(
        PCollection.IsBounded.BOUNDED,
        unboundedPC
            .apply(new SplittableParDo<>("unbounded to unbounded", unboundedFn))
            .isBounded());
  }
}
