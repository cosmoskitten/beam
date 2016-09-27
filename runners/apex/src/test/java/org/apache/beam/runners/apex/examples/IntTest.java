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

package org.apache.beam.runners.apex.examples;


  import static org.apache.beam.sdk.TestUtils.LINES;
import static org.apache.beam.sdk.TestUtils.LINES2;
import static org.apache.beam.sdk.TestUtils.NO_LINES;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
  import static org.hamcrest.Matchers.is;
  import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.TestApexRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.io.CountingInput.UnboundedCountingInput;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
  import org.apache.beam.sdk.testing.PAssert;
  import org.apache.beam.sdk.testing.RunnableOnService;
  import org.apache.beam.sdk.testing.TestPipeline;
  import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Max;
  import org.apache.beam.sdk.transforms.Min;
  import org.apache.beam.sdk.transforms.PTransform;
  import org.apache.beam.sdk.transforms.ParDo;
  import org.apache.beam.sdk.transforms.RemoveDuplicates;
  import org.apache.beam.sdk.transforms.SerializableFunction;
  import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;
  import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;
  import org.junit.experimental.categories.Category;
  import org.junit.runner.RunWith;
  import org.junit.runners.JUnit4;

  /**
   * Tests for {@link CountingInput}.
   */
  @Ignore
  @RunWith(JUnit4.class)
  public class IntTest {
    public static void addCountingAsserts(PCollection<Long> input, long numElements) {
      // Count == numElements
      PAssert.thatSingleton(input.apply("Count", Count.<Long>globally()))
          .isEqualTo(numElements);
      // Unique count == numElements
      PAssert.thatSingleton(
              input
                  .apply(RemoveDuplicates.<Long>create())
                  .apply("UniqueCount", Count.<Long>globally()))
          .isEqualTo(numElements);
      // Min == 0
      PAssert.thatSingleton(input.apply("Min", Min.<Long>globally())).isEqualTo(0L);
      // Max == numElements-1
      PAssert.thatSingleton(input.apply("Max", Max.<Long>globally()))
          .isEqualTo(numElements - 1);
    }

    @Test
    @Category(RunnableOnService.class)
    public void testBoundedInput() {
      //Pipeline p = TestPipeline.create();
      ApexPipelineOptions options = PipelineOptionsFactory.as(ApexPipelineOptions.class);
      options.setRunner(TestApexRunner.class);
      Pipeline p = Pipeline.create(options);

      long numElements = 1000;
      PCollection<Long> input = p.apply(CountingInput.upTo(numElements));

      addCountingAsserts(input, numElements);
      p.run();
      System.out.println("running");
    }

    @Test
    public void testBoundedDisplayData() {
      PTransform<?, ?> input = CountingInput.upTo(1234);
      DisplayData displayData = DisplayData.from(input);
      assertThat(displayData, hasDisplayItem("upTo", 1234));
    }

    @Test
    @Category(RunnableOnService.class)
    public void testUnboundedInput() {
      //Pipeline p = TestPipeline.create();
      ApexPipelineOptions options = PipelineOptionsFactory.as(ApexPipelineOptions.class);
      options.setRunner(TestApexRunner.class);
      Pipeline p = Pipeline.create(options);


      long numElements = 1000;

      PCollection<Long> input = p.apply(CountingInput.unbounded().withMaxNumRecords(numElements));

//      input = input.apply(Window.<Long>into(FixedWindows.of(Duration.standardSeconds(10))));

      addCountingAsserts(input, numElements);
      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testUnboundedInputRate() {
      Pipeline p = TestPipeline.create();
      long numElements = 5000;

      long elemsPerPeriod = 10L;
      Duration periodLength = Duration.millis(8);
      PCollection<Long> input =
          p.apply(
              CountingInput.unbounded()
                  .withRate(elemsPerPeriod, periodLength)
                  .withMaxNumRecords(numElements));

      addCountingAsserts(input, numElements);
      long expectedRuntimeMillis = (periodLength.getMillis() * numElements) / elemsPerPeriod;
      Instant startTime = Instant.now();
      p.run();
      Instant endTime = Instant.now();
      assertThat(endTime.isAfter(startTime.plus(expectedRuntimeMillis)), is(true));
    }

    private static class ElementValueDiff extends DoFn<Long, Long> {
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        c.output(c.element() - c.timestamp().getMillis());
      }
    }

    @Test
    @Category(RunnableOnService.class)
    public void testUnboundedInputTimestamps() {
      Pipeline p = TestPipeline.create();
      long numElements = 1000;

      PCollection<Long> input =
          p.apply(
              CountingInput.unbounded()
                  .withTimestampFn(new ValueAsTimestampFn())
                  .withMaxNumRecords(numElements));
      addCountingAsserts(input, numElements);

      PCollection<Long> diffs =
          input
              .apply("TimestampDiff", ParDo.of(new ElementValueDiff()))
              .apply("RemoveDuplicateTimestamps", RemoveDuplicates.<Long>create());
      // This assert also confirms that diffs only has one unique value.
      PAssert.thatSingleton(diffs).isEqualTo(0L);

      p.run();
    }

    @Test
    public void testUnboundedDisplayData() {
      Duration maxReadTime = Duration.standardHours(5);
      SerializableFunction<Long, Instant> timestampFn = new SerializableFunction<Long, Instant>() {
        @Override
        public Instant apply(Long input) {
          return Instant.now();
        }
      };

      PTransform<?, ?> input = CountingInput.unbounded()
          .withMaxNumRecords(1234)
          .withMaxReadTime(maxReadTime)
          .withTimestampFn(timestampFn);

      DisplayData displayData = DisplayData.from(input);

      assertThat(displayData, hasDisplayItem("maxRecords", 1234));
      assertThat(displayData, hasDisplayItem("maxReadTime", maxReadTime));
      assertThat(displayData, hasDisplayItem("timestampFn", timestampFn.getClass()));
    }

    /**
     * A timestamp function that uses the given value as the timestamp. Because the input values will
     * not wrap, this function is non-decreasing and meets the timestamp function criteria laid out
     * in {@link UnboundedCountingInput#withTimestampFn(SerializableFunction)}.
     */
    private static class ValueAsTimestampFn implements SerializableFunction<Long, Instant> {
      @Override
      public Instant apply(Long input) {
        return new Instant(input);
      }
    }








    private PCollectionList<String> makePCollectionListOfStrings(
        Pipeline p,
        List<List<String>> lists) {
      return makePCollectionList(p, StringUtf8Coder.of(), lists);
    }

    private <T> PCollectionList<T> makePCollectionList(
        Pipeline p,
        Coder<T> coder,
        List<List<T>> lists) {
      List<PCollection<T>> pcs = new ArrayList<>();
      int index = 0;
      for (List<T> list : lists) {
        PCollection<T> pc = p.apply("Create" + (index++), Create.of(list).withCoder(coder));
        pcs.add(pc);
      }
      return PCollectionList.of(pcs);
    }

    private <T> List<T> flattenLists(List<List<T>> lists) {
      List<T> flattened = new ArrayList<>();
      for (List<T> list : lists) {
        flattened.addAll(list);
      }
      return flattened;
    }




    @Test
    @Category(RunnableOnService.class)
    public void testFlattenPCollectionList() {
//      Pipeline p = TestPipeline.create();
      ApexPipelineOptions options = PipelineOptionsFactory.as(ApexPipelineOptions.class);
      options.setRunner(TestApexRunner.class);
      Pipeline p = Pipeline.create(options);

      List<List<String>> inputs = Arrays.asList(
        LINES, NO_LINES, LINES2, NO_LINES, LINES, NO_LINES);

      PCollection<String> output =
          makePCollectionListOfStrings(p, inputs)
          .apply(Flatten.<String>pCollections());

      PAssert.that(output).containsInAnyOrder(flattenLists(inputs));
      p.run();
    }





}
