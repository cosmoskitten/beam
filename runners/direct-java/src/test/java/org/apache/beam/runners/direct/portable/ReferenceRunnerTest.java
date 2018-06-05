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

package org.apache.beam.runners.direct.portable;

import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.resume;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.stop;

import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.apache.beam.runners.core.construction.JavaReadViaImpulse;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.direct.ParDoMultiOverrideFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the {@link ReferenceRunner}. */
@RunWith(JUnit4.class)
public class ReferenceRunnerTest implements Serializable {
  @Test
  public void pipelineExecution() throws Exception {
    Pipeline p = Pipeline.create();
    TupleTag<KV<String, Integer>> food = new TupleTag<>();
    TupleTag<Integer> originals = new TupleTag<Integer>() {};
    PCollectionTuple parDoOutputs =
        p.apply(Create.of(1, 2, 3))
            .apply(
                ParDo.of(
                        new DoFn<Integer, KV<String, Integer>>() {
                          @ProcessElement
                          public void process(ProcessContext ctxt) {
                            for (int i = 0; i < ctxt.element(); i++) {
                              ctxt.outputWithTimestamp(
                                  KV.of("foo", ctxt.element()),
                                  new Instant(0).plus(Duration.standardHours(i)));
                            }
                            ctxt.output(originals, ctxt.element());
                          }
                        })
                    .withOutputTags(food, TupleTagList.of(originals)));
    FixedWindows windowFn = FixedWindows.of(Duration.standardMinutes(5L));
    PCollection<KV<String, Set<Integer>>> grouped =
        parDoOutputs
            .get(food)
            .apply(Window.into(windowFn))
            .apply(GroupByKey.create())
            .apply(
                ParDo.of(
                    new DoFn<KV<String, Iterable<Integer>>, KV<String, Set<Integer>>>() {
                      @ProcessElement
                      public void process(ProcessContext ctxt) {
                        ctxt.output(
                            KV.of(
                                ctxt.element().getKey(),
                                ImmutableSet.copyOf(ctxt.element().getValue())));
                      }
                    }));

    PAssert.that(grouped)
        .containsInAnyOrder(
            KV.of("foo", ImmutableSet.of(1, 2, 3)),
            KV.of("foo", ImmutableSet.of(2, 3)),
            KV.of("foo", ImmutableSet.of(3)));

    p.replaceAll(Collections.singletonList(JavaReadViaImpulse.boundedOverride()));

    ReferenceRunner runner =
        ReferenceRunner.forInProcessPipeline(
            PipelineTranslation.toProto(p),
            PipelineOptionsTranslation.toProto(PipelineOptionsFactory.create()));
    runner.execute();
  }

  static class PairStringWithIndexToLength extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public ProcessContinuation process(ProcessContext c, OffsetRangeTracker tracker) {
      for (long i = tracker.currentRestriction().getFrom(), numIterations = 0;
          tracker.tryClaim(i);
          ++i, ++numIterations) {
        c.output(KV.of(c.element(), (int) i));
        if (numIterations % 3 == 0) {
          return resume();
        }
      }
      return stop();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRange(String element) {
      return new OffsetRange(0, element.length());
    }

    @SplitRestriction
    public void splitRange(
        String element, OffsetRange range, OutputReceiver<OffsetRange> receiver) {
      receiver.output(new OffsetRange(range.getFrom(), (range.getFrom() + range.getTo()) / 2));
      receiver.output(new OffsetRange((range.getFrom() + range.getTo()) / 2, range.getTo()));
    }
  }

  @Test
  public void testSDF() throws Exception {
    Pipeline p = Pipeline.create();

    PCollection<KV<String, Integer>> res =
        p.apply(Create.of("a", "bb", "ccccc"))
            .apply(ParDo.of(new PairStringWithIndexToLength()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));

    PAssert.that(res)
        .containsInAnyOrder(
            Arrays.asList(
                KV.of("a", 0),
                KV.of("bb", 0),
                KV.of("bb", 1),
                KV.of("ccccc", 0),
                KV.of("ccccc", 1),
                KV.of("ccccc", 2),
                KV.of("ccccc", 3),
                KV.of("ccccc", 4)));

    p.replaceAll(
        Arrays.asList(
            JavaReadViaImpulse.boundedOverride(),
            PTransformOverride.of(
                PTransformMatchers.splittableParDo(), new ParDoMultiOverrideFactory())));

    ReferenceRunner runner =
        ReferenceRunner.forInProcessPipeline(
            PipelineTranslation.toProto(p),
            PipelineOptionsTranslation.toProto(PipelineOptionsFactory.create()));
    runner.execute();
  }
}
