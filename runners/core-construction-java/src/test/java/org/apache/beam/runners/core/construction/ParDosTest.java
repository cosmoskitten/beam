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

package org.apache.beam.runners.core.construction;

import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.ParDoPayload;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineLongFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link ParDos}. */
@RunWith(Parameterized.class)
public class ParDosTest {
  @Parameters(name = "{index}: {0}")
  public static Iterable<ParDo.MultiOutput<?, ?>> data() {
    TestPipeline p = TestPipeline.create();
    PCollectionView<Long> singletonSideInput =
        p.apply(GenerateSequence.from(0L).to(1L)).apply(View.<Long>asSingleton());
    PCollectionView<Map<Long, Iterable<String>>> multimapSideInput =
        p.apply(Create.of(KV.of(1L, "foo"), KV.of(1L, "bar"), KV.of(2L, "spam")))
            .apply(View.<Long, String>asMultimap());

    return ImmutableList.<ParDo.MultiOutput<?, ?>>of(
        ParDo.of(new DropElementsFn()).withOutputTags(new TupleTag<Void>(), TupleTagList.empty()),
        ParDo.of(new DropElementsFn())
            .withOutputTags(new TupleTag<Void>(), TupleTagList.empty())
            .withSideInputs(singletonSideInput, multimapSideInput),
        ParDo.of(new DropElementsFn())
            .withOutputTags(
                new TupleTag<Void>(),
                TupleTagList.of(new TupleTag<Object>()).and(new TupleTag<Integer>()))
            .withSideInputs(singletonSideInput, multimapSideInput),
        ParDo.of(new DropElementsFn())
            .withOutputTags(
                new TupleTag<Void>(),
                TupleTagList.of(new TupleTag<Object>()).and(new TupleTag<Integer>())));
  }

  @Parameter(0)
  public ParDo.MultiOutput<?, ?> parDo;

  @Test
  public void testToAndFromProto() throws InvalidProtocolBufferException {
    SdkComponents components = SdkComponents.create();
    ParDoPayload payload = ParDos.toProto(parDo, components);

    assertThat(ParDos.getDoFn(payload), Matchers.<DoFn<?, ?>>equalTo(parDo.getFn()));
    assertThat(
        ParDos.getMainOutputTag(payload), Matchers.<TupleTag<?>>equalTo(parDo.getMainOutputTag()));
    for (PCollectionView<?> view : parDo.getSideInputs()) {
      payload.getSideInputsOrThrow(view.getTagInternal().getId());
    }
  }

  private static class DropElementsFn extends DoFn<KV<Long, String>, Void> {
    private static final String BAG_STATE_ID = "bagState";
    private static final String COMBINING_STATE_ID = "combiningState";
    private static final String EVENT_TIMER_ID = "eventTimer";
    private static final String PROCESSING_TIMER_ID = "processingTimer";

    @StateId(BAG_STATE_ID)
    private final StateSpec<BagState<String>> bagState = StateSpecs.bag(StringUtf8Coder.of());

    @StateId(COMBINING_STATE_ID)
    private final StateSpec<CombiningState<Long, long[], Long>> combiningState =
        StateSpecs.combining(
            new BinaryCombineLongFn() {
              @Override
              public long apply(long left, long right) {
                return Math.max(left, right);
              }

              @Override
              public long identity() {
                return Long.MIN_VALUE;
              }
            });

    @TimerId(EVENT_TIMER_ID)
    private final TimerSpec eventTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId(PROCESSING_TIMER_ID)
    private final TimerSpec processingTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void dropInput(
        ProcessContext context,
        BoundedWindow window,
        @StateId(BAG_STATE_ID) BagState<String> bagStateState,
        @StateId(COMBINING_STATE_ID) CombiningState<Long, long[], Long> combiningStateState,
        @TimerId(EVENT_TIMER_ID) Timer eventTimerTimer,
        @TimerId(PROCESSING_TIMER_ID) Timer processingTimerTimer) {
      context.output(null);
    }

    @OnTimer(EVENT_TIMER_ID)
    public void onEventTime(OnTimerContext context) {}

    @OnTimer(PROCESSING_TIMER_ID)
    public void onProcessingTime(OnTimerContext context) {}

    @Override
    public boolean equals(Object other) {
      return other instanceof DropElementsFn;
    }

    @Override
    public int hashCode() {
      return DropElementsFn.class.hashCode();
    }
  }
}
