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
package org.apache.beam.examples;

import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Date;
import org.apache.beam.examples.WindowedWordCount.Options;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.BigqueryMatcher;
import org.apache.beam.sdk.testing.FileChecksumMatcher;
import org.apache.beam.sdk.testing.StreamingIT;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end integration test of {@link WindowedWordCount}.
 */
@RunWith(JUnit4.class)
public class WindowedWordCountIT {

  private static final String DEFAULT_OUTPUT_CHECKSUM = "ff54f6f42b2afeb146206c1e8e915deaee0362b4";

  /**
   * Options for the {@link WindowedWordCount} Integration Test.
   */
  public interface WindowedWordCountITOptions
      extends Options, TestPipelineOptions, StreamingOptions {
    String getChecksum();
    void setChecksum(String value);
  }

  @Test
  public void testWindowedWordCountInBatch() throws Exception {
    testWindowedWordCountPipeline(defaultOptions());
  }

  @Test
  @Category(StreamingIT.class)
  public void testWindowedWordCountInStreaming() throws Exception {
    testWindowedWordCountPipeline(streamingOptions());
  }

  private WindowedWordCountITOptions defaultOptions() throws Exception {
    PipelineOptionsFactory.register(WindowedWordCountITOptions.class);
    WindowedWordCountITOptions options =
        TestPipeline.testingPipelineOptions().as(WindowedWordCountITOptions.class);
    options.setOutput(
        IOChannelUtils.resolve(
            options.getTempRoot(),
            String.format("WordCountIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
            "output",
            "results"));
    return options;
  }

  private WindowedWordCountITOptions streamingOptions() throws Exception {
    WindowedWordCountITOptions options = defaultOptions();
    options.setStreaming(true);
    return options;
  }

  private WindowedWordCountITOptions batchOptions() throws Exception {
    WindowedWordCountITOptions options = defaultOptions();
    // This is the default value, but make it explicit
    options.setStreaming(false);
    return options;
  }

  private void testWindowedWordCountPipeline(WindowedWordCountITOptions options)
      throws IOException {

    String outputChecksum =
        Strings.isNullOrEmpty(options.getChecksum())
            ? DEFAULT_OUTPUT_CHECKSUM
            : options.getChecksum();
    options.setOnSuccessMatcher(
        new FileChecksumMatcher(outputChecksum, options.getOutput() + "*"));

    WindowedWordCount.main(TestPipeline.convertToArgs(options));
  }
}
