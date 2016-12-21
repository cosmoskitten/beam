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
package org.apache.beam.runners.dataflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests that the Dataflow runner accepts various --XLocation options with and without trailing '/'.
 */
@RunWith(JUnit4.class)
public class DataflowLocationIT {
  private static void runTestPipeline(DataflowPipelineOptions options) {
    Pipeline p = Pipeline.create(options);
    PCollection<Integer> ints = p.apply(Create.of(1, 2, 3));

    final PCollectionView<Integer> singletonSum =
        ints.apply(Sum.integersGlobally())
            .apply(View.<Integer>asSingleton());

    PCollection<String> sumPlusValue =
        ints.apply(ParDo.of(new DoFn<Integer, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            Integer sumPlusValue = c.element() + c.sideInput(singletonSum);
            c.output(sumPlusValue.toString());
          }
        }).withSideInputs(singletonSum));

    PAssert.that(sumPlusValue).containsInAnyOrder("7", "8", "9");

    p.run().waitUntilFinish();
  }

  private static String ensureSlash(String input) {
    if (!input.endsWith("/")) {
      return input + '/';
    }
    return input;
  }

  private static String ensureNoSlash(String input) {
    if (input.endsWith("/")) {
      return input.substring(0, input.length() - 1);
    }
    return input;
  }

  @Test
  public void testWithTrailingSlash() throws Exception {
    DataflowPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowPipelineOptions.class);
    options.setTempLocation(ensureSlash(options.getTempLocation()));
    options.setGcpTempLocation(ensureSlash(options.getGcpTempLocation()));
    options.setStagingLocation(ensureSlash(options.getStagingLocation()));
    runTestPipeline(options);
  }

  @Test
  public void testWithNoTrailingSlash() throws Exception {
    DataflowPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowPipelineOptions.class);
    options.setTempLocation(ensureNoSlash(options.getTempLocation()));
    options.setGcpTempLocation(ensureNoSlash(options.getGcpTempLocation()));
    options.setStagingLocation(ensureNoSlash(options.getStagingLocation()));
    runTestPipeline(options);
  }
}
