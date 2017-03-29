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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ParDos}.
 */
@RunWith(JUnit4.class)
public class ParDosTest {
  @Rule public TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
  @Rule public ExpectedException thrown = ExpectedException.none();
  private PCollection<Long> mainInput = pipeline.apply(CountingInput.unbounded());
  private PCollectionView<String> sideInput =
      pipeline.apply(Create.of("foo")).apply(View.<String>asSingleton());

  @Test
  public void getMainInputSingleOutputSingleInput() {
    PCollection<Long> input =
        ParDos.getMainInput(mainInput.expand(), ParDo.of(new TestDoFn()));
    assertThat(input, equalTo(mainInput));
  }

  @Test
  public void getMainInputSingleOutputSideInputs() {
    ImmutableMap<TupleTag<?>, PValue> inputs =
        ImmutableMap.<TupleTag<?>, PValue>builder().putAll(mainInput.expand())
            .put(sideInput.getTagInternal(), sideInput.getPCollection())
            .build();
    PCollection<Long> input =
        ParDos.getMainInput(
            inputs, ParDo.of(new TestDoFn()).withSideInputs(sideInput));
    assertThat(input, equalTo(mainInput));
  }

  @Test
  public void getMainInputMultiOutputSingleInput() {
    PCollection<Long> input =
        ParDos.getMainInput(
            mainInput.expand(),
            ParDo.of(new TestDoFn())
                .withOutputTags(new TupleTag<Long>(), TupleTagList.of(new TupleTag<Object>())));
    assertThat(input, equalTo(mainInput));
  }

  @Test
  public void getMainInputMultiOutputSideInputs() {
    ImmutableMap<TupleTag<?>, PValue> inputs =
        ImmutableMap.<TupleTag<?>, PValue>builder()
            .putAll(mainInput.expand())
            .put(sideInput.getTagInternal(), sideInput.getPCollection())
            .build();
    PCollection<Long> input =
        ParDos.getMainInput(
            inputs,
            ParDo.of(new TestDoFn())
                .withOutputTags(new TupleTag<Long>(), TupleTagList.of(new TupleTag<Object>()))
                .withSideInputs(sideInput));
    assertThat(input, equalTo(mainInput));
  }

  @Test
  public void getMainInputExtraMainInputsThrows() {
    PCollection<Long> notInParDo = pipeline.apply("otherPCollection", Create.of(1L, 2L, 3L));
    ImmutableMap<TupleTag<?>, PValue> inputs =
        ImmutableMap.<TupleTag<?>, PValue>builder()
            .putAll(mainInput.expand())
            // Not represnted as an input
            .put(new TupleTag<Long>(), notInParDo)
            .put(sideInput.getTagInternal(), sideInput.getPCollection())
            .build();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("multiple inputs");
    thrown.expectMessage("not side inputs");
    thrown.expectMessage(mainInput.toString());
    thrown.expectMessage(notInParDo.toString());
    ParDos.getMainInput(
        inputs,
        ParDo.of(new TestDoFn())
            .withSideInputs(sideInput)
            .withOutputTags(new TupleTag<Long>(), TupleTagList.of(new TupleTag<Object>())));
  }

  @Test
  public void getMainInputNoMainInputsThrows() {
    ImmutableMap<TupleTag<?>, PValue> inputs =
        ImmutableMap.<TupleTag<?>, PValue>builder()
            .put(sideInput.getTagInternal(), sideInput.getPCollection())
            .build();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No main input");
    ParDos.getMainInput(
        inputs,
        ParDo.of(new TestDoFn())
            .withSideInputs(sideInput)
            .withOutputTags(new TupleTag<Long>(), TupleTagList.of(new TupleTag<Object>())));
  }

  private static class TestDoFn extends DoFn<Long, Long> {
    @ProcessElement public void process(ProcessContext context) {}
  }
}
