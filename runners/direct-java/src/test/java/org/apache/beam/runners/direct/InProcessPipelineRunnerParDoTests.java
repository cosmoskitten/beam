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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.IllegalMutationException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

/**
 * Tests of {@link ParDo} when executed in the {@link InProcessPipelineRunner}.
 */
public class InProcessPipelineRunnerParDoTests {
  @Rule public ExpectedException thrown = ExpectedException.none();
  /**
   * Tests that a {@link DoFn} that mutates an output with a good equals() fails in the
   * {@link InProcessPipelineRunner}.
   */
  @Test
  public void testMutatingOutputThenOutputDoFnError() throws Exception {
    Pipeline pipeline = createPipeline();

    pipeline
        .apply(Create.of(42))
        .apply(ParDo.of(new DoFn<Integer, List<Integer>>() {
          @Override public void processElement(ProcessContext c) {
            List<Integer> outputList = Arrays.asList(1, 2, 3, 4);
            c.output(outputList);
            outputList.set(0, 37);
            c.output(outputList);
          }
        }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("output");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an output with a good equals() fails in the
   * {@link InProcessPipelineRunner}.
   */
  @Test
  public void testMutatingOutputThenTerminateDoFnError() throws Exception {
    Pipeline pipeline = createPipeline();

    pipeline
        .apply(Create.of(42))
        .apply(ParDo.of(new DoFn<Integer, List<Integer>>() {
          @Override public void processElement(ProcessContext c) {
            List<Integer> outputList = Arrays.asList(1, 2, 3, 4);
            c.output(outputList);
            outputList.set(0, 37);
          }
        }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("output");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an output with a bad equals() still fails
   * in the {@link InProcessPipelineRunner}.
   */
  @Test
  public void testMutatingOutputCoderDoFnError() throws Exception {
    Pipeline pipeline = createPipeline();

    pipeline
        .apply(Create.of(42))
        .apply(ParDo.of(new DoFn<Integer, byte[]>() {
          @Override public void processElement(ProcessContext c) {
            byte[] outputArray = new byte[]{0x1, 0x2, 0x3};
            c.output(outputArray);
            outputArray[0] = 0xa;
            c.output(outputArray);
          }
        }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("output");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates its input with a good equals() fails in the
   * {@link InProcessPipelineRunner}.
   */
  @Test
  public void testMutatingInputDoFnError() throws Exception {
    Pipeline pipeline = createPipeline();

    pipeline
        .apply(Create.of(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6))
            .withCoder(ListCoder.of(VarIntCoder.of())))
        .apply(ParDo.of(new DoFn<List<Integer>, Integer>() {
          @Override public void processElement(ProcessContext c) {
            List<Integer> inputList = c.element();
            inputList.set(0, 37);
            c.output(12);
          }
        }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("input");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an input with a bad equals() still fails
   * in the {@link InProcessPipelineRunner}.
   */
  @Test
  public void testMutatingInputCoderDoFnError() throws Exception {
    Pipeline pipeline = createPipeline();

    pipeline
        .apply(Create.of(new byte[]{0x1, 0x2, 0x3}, new byte[]{0x4, 0x5, 0x6}))
        .apply(ParDo.of(new DoFn<byte[], Integer>() {
          @Override public void processElement(ProcessContext c) {
            byte[] inputArray = c.element();
            inputArray[0] = 0xa;
            c.output(13);
          }
        }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("input");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  private Pipeline createPipeline() {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(InProcessPipelineRunner.class);
    return Pipeline.create(opts);
  }
}
