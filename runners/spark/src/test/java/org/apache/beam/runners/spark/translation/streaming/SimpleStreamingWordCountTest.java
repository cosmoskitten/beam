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
package org.apache.beam.runners.spark.translation.streaming;


import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.examples.WordCount;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.runners.spark.translation.streaming.utils.PAssertStreaming;
import org.apache.beam.runners.spark.translation.streaming.utils.TestOptionsForStreaming;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;

import org.joda.time.Duration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Simple word count streaming test.
 */
public class SimpleStreamingWordCountTest implements Serializable {

  @Rule
  public TemporaryFolder checkpointParentDir = new TemporaryFolder();

  @Rule
  public TestOptionsForStreaming commonOptions = new TestOptionsForStreaming();

  private static final String[] WORDS = {"hi there", "hi", "hi sue bob", "hi sue", "", "bob hi"};

  private static final List<Iterable<String>> MANY_WORDS =
      Lists.<Iterable<String>>newArrayList(Arrays.asList(WORDS), Arrays.asList(WORDS));

  private static final String[] EXPECTED_WORD_COUNTS = {"hi: 5", "there: 1", "sue: 2", "bob: 2"};

  private static final Duration BATCH_INTERVAL_MILLIS = Duration.standardSeconds(1);

  @Test
  public void testRun() throws Exception {

    SparkPipelineOptions options = commonOptions.withTmpCheckpointDir(
        checkpointParentDir.newFolder(getClass().getSimpleName()));

    // override defaults
    options.setBatchIntervalMillis(BATCH_INTERVAL_MILLIS.getMillis());
    options.setTimeout(BATCH_INTERVAL_MILLIS.multipliedBy(3).getMillis());

    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> output =
        pipeline
            .apply(CreateStream.fromQueue(MANY_WORDS))
            .setCoder(StringUtf8Coder.of())
            .apply(Window.<String>into(FixedWindows.of(BATCH_INTERVAL_MILLIS.multipliedBy(2))))
            .apply(new WordCount.CountWords())
            .apply(MapElements.via(new WordCount.FormatAsTextFn()));

    PAssertStreaming.assertContents(output, EXPECTED_WORD_COUNTS);

    EvaluationResult res = (EvaluationResult) pipeline.run();
    res.close();
  }
}
