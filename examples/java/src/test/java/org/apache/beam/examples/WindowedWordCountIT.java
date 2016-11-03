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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import org.apache.beam.examples.common.WriteWindowedFilesDoFn;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.FileChecksumMatcher;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.StreamingIT;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.ExplicitShardedFile;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** End-to-end integration test of {@link WindowedWordCount}. */
@RunWith(JUnit4.class)
public class WindowedWordCountIT {

  private static final String DEFAULT_INPUT =
      "gs://apache-beam-samples/shakespeare/winterstale-personae";
  private static final String DEFAULT_OUTPUT_CHECKSUM = "cd5b52939257e12428a9fa085c32a84dd209b180";

  /** Options for the {@link WindowedWordCount} Integration Test. */
  public interface WindowedWordCountITOptions
      extends WindowedWordCount.Options, TestPipelineOptions, StreamingOptions {}

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
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
    WindowedWordCountITOptions options =
        TestPipeline.testingPipelineOptions().as(WindowedWordCountITOptions.class);
    options.setInputFile(DEFAULT_INPUT);
    options.setTestTimeoutSeconds(1200L);

    options.setMinTimestampMillis(0L);
    options.setMinTimestampMillis(Duration.standardHours(1).getMillis());
    options.setWindowSize(10);

    options.setOutput(
        IOChannelUtils.resolve(
            options.getTempRoot(),
            String.format("WindowedWordCountIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
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

    String outputPrefix = options.getOutput();

    List<String> expectedOutputFiles = Lists.newArrayListWithCapacity(6);
    for (int startMinute : ImmutableList.of(0, 10, 20, 30, 40, 50)) {
      Instant windowStart =
          new Instant(options.getMinTimestampMillis()).plus(Duration.standardMinutes(startMinute));
      expectedOutputFiles.add(
          WriteWindowedFilesDoFn.fileForWindow(
              outputPrefix,
              new IntervalWindow(windowStart, windowStart.plus(Duration.standardMinutes(10)))));
    }

    options.setOnSuccessMatcher(
        new CountFilesChecksumMatcher(DEFAULT_OUTPUT_CHECKSUM, expectedOutputFiles));

    WindowedWordCount.main(TestPipeline.convertToArgs(options));
  }

  /**
   * A checksum matcher that reads a bunch of text files, splits the lines on colons, adds up the
   * wordcounts, and then checksums the results.
   */
  private static class CountFilesChecksumMatcher extends TypeSafeMatcher<PipelineResult>
      implements SerializableMatcher<PipelineResult> {

    private static final Logger LOG = LoggerFactory.getLogger(FileChecksumMatcher.class);

    private final String expectedChecksum;
    private final Collection<String> filePaths;
    private String actualChecksum;

    public CountFilesChecksumMatcher(String expectedChecksum, Collection<String> filePaths) {
      checkArgument(
          !Strings.isNullOrEmpty(expectedChecksum),
          "Expected valid checksum, but received %s",
          expectedChecksum);

      this.expectedChecksum = expectedChecksum;
      this.filePaths = filePaths;
    }

    @Override
    public boolean matchesSafely(PipelineResult pipelineResult) {
      ExplicitShardedFile shardedFile = new ExplicitShardedFile(filePaths);

      try {
        // Load output data
        List<String> lines = shardedFile.readFilesWithRetries();

        // Since the windowing is nondeterministic we only check the sums
        SortedMap<String, Long> counts = Maps.newTreeMap();
        for (String line : lines) {
          String[] splits = line.split(": ");
          String word = splits[0];
          long count = Long.parseLong(splits[1]);

          Long current = counts.get(word);
          if (current == null) {
            counts.put(word, count);
          } else {
            counts.put(word, current + count);
          }
        }

        // Verify outputs. Checksum is computed using SHA-1 algorithm
        actualChecksum = hashing(counts);
        LOG.info("Generated checksum for output data: {}", actualChecksum);

        return actualChecksum.equals(expectedChecksum);
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to read from path: %s", filePaths));
      }
    }

    private String hashing(SortedMap<String, Long> counts) {
      List<HashCode> hashCodes = new ArrayList<>();
      for (Map.Entry<String, Long> entry : counts.entrySet()) {
        hashCodes.add(Hashing.sha1().hashString(entry.getKey(), StandardCharsets.UTF_8));
        hashCodes.add(Hashing.sha1().hashLong(entry.getValue()));
      }
      return Hashing.combineOrdered(hashCodes).toString();
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("Expected checksum is (").appendText(expectedChecksum).appendText(")");
    }

    @Override
    public void describeMismatchSafely(PipelineResult pResult, Description description) {
      description.appendText("was (").appendText(actualChecksum).appendText(")");
    }
  }
}
