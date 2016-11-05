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
package org.apache.beam.runners.flink.examples.streaming;

import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedSocketSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

/**
 * To run the example, first open two sockets on two terminals by executing the commands:
 * <ul>
 *   <li><code>nc -lk 9999</code>, and
 *   <li><code>nc -lk 9998</code>
 * </ul>
 * and then launch the example. Now whatever you type in the terminal is going to be
 * the input to the program.
 * */
public class JoinExamples {

  static PCollection<String> joinEvents(PCollection<String> streamA,
                      PCollection<String> streamB) throws Exception {

    final TupleTag<String> firstInfoTag = new TupleTag<>();
    final TupleTag<String> secondInfoTag = new TupleTag<>();

    // transform both input collections to tuple collections, where the keys are country
    // codes in both cases.
    PCollection<KV<String, String>> firstInfo = streamA.apply(
        ParDo.of(new ExtractEventDataFn()));
    PCollection<KV<String, String>> secondInfo = streamB.apply(
        ParDo.of(new ExtractEventDataFn()));

    // country code 'key' -> CGBKR (<event info>, <country name>)
    PCollection<KV<String, CoGbkResult>> kvpCollection = KeyedPCollectionTuple
        .of(firstInfoTag, firstInfo)
        .and(secondInfoTag, secondInfo)
        .apply(CoGroupByKey.<String>create());

    // Process the CoGbkResult elements generated by the CoGroupByKey transform.
    // country code 'key' -> string of <event info>, <country name>
    PCollection<KV<String, String>> finalResultCollection =
        kvpCollection.apply("Process", ParDo.of(
            new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
              private static final long serialVersionUID = 0;

              @ProcessElement
              public void processElement(ProcessContext c) {
                KV<String, CoGbkResult> e = c.element();
                String key = e.getKey();

                String defaultA = "NO_VALUE";

                // the following getOnly is a bit tricky because it expects to have
                // EXACTLY ONE value in the corresponding stream and for the corresponding key.

                String lineA = e.getValue().getOnly(firstInfoTag, defaultA);
                for (String lineB : c.element().getValue().getAll(secondInfoTag)) {
                  // Generate a string that combines information from both collection values
                  c.output(KV.of(key, "Value A: " + lineA + " - Value B: " + lineB));
                }
              }
            }));

    return finalResultCollection
        .apply("Format", ParDo.of(new DoFn<KV<String, String>, String>() {
          private static final long serialVersionUID = 0;

          @ProcessElement
          public void processElement(ProcessContext c) {
            String result = c.element().getKey() + " -> " + c.element().getValue();
            System.out.println(result);
            c.output(result);
          }
        }));
  }

  static class ExtractEventDataFn extends DoFn<String, KV<String, String>> {
    private static final long serialVersionUID = 0;

    @ProcessElement
    public void processElement(ProcessContext c) {
      String line = c.element().toLowerCase();
      String key = line.split("\\s")[0];
      c.output(KV.of(key, line));
    }
  }

  private interface Options extends WindowedWordCount.StreamingWordCountOptions {

  }

  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);
    options.setCheckpointingInterval(1000L);
    options.setNumberOfExecutionRetries(5);
    options.setExecutionRetryDelay(3000L);
    options.setRunner(FlinkRunner.class);

    WindowFn<Object, ?> windowFn = FixedWindows.of(
        Duration.standardSeconds(options.getWindowSize()));

    Pipeline p = Pipeline.create(options);

    // the following two 'applys' create multiple inputs to our pipeline, one for each
    // of our two input sources.
    PCollection<String> streamA = p
        .apply("FirstStream", Read.from(new UnboundedSocketSource<>("localhost", 9999, '\n', 3)))
        .apply(Window.<String>into(windowFn)
            .triggering(AfterWatermark.pastEndOfWindow()).withAllowedLateness(Duration.ZERO)
            .discardingFiredPanes());
    PCollection<String> streamB = p
        .apply("SecondStream", Read.from(new UnboundedSocketSource<>("localhost", 9998, '\n', 3)))
        .apply(Window.<String>into(windowFn)
            .triggering(AfterWatermark.pastEndOfWindow()).withAllowedLateness(Duration.ZERO)
            .discardingFiredPanes());

    PCollection<String> formattedResults = joinEvents(streamA, streamB);
    formattedResults.apply(TextIO.Write.to("./outputJoin.txt"));
    p.run();
  }

}
