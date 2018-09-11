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

package org.apache.beam.examples.complete;

import java.net.URI;
import java.util.Date;
import org.apache.beam.examples.complete.TfIdf.ComputeTfIdf;
import org.apache.beam.examples.complete.TfIdf.Options;
import org.apache.beam.examples.complete.TfIdf.ReadDocuments;
import org.apache.beam.examples.complete.TfIdf.WriteTfIdf;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.FileChecksumMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for TfIdf example. */
@RunWith(JUnit4.class)
public class TfIdfIT {

  private static String DEFAULT_INPUT = "gs://apache-beam-samples/shakespeare/";
  private static String OUTPUT_FILE =
      "/tfidf-it-output/tfidf-result" + Long.toString(System.currentTimeMillis());
  private static final String EXPECTED_OUTPUT_CHECKSUM = "be89ba2808da9cb76ee0101418690cc48953fd74";

  /**
   * Options for the TfIdf Integration Test.
   *
   * <p>Define expected output file checksum to verify TfIdf pipeline result with customized input.
   */
  public interface TfIdfITOptions extends TestPipelineOptions, Options {}

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TfIdfITOptions.class);
  }

  @Test
  public void testE2ETfIdf() throws Exception {
    TfIdfITOptions options = TestPipeline.testingPipelineOptions().as(TfIdfITOptions.class);
    options.setInput(DEFAULT_INPUT);
    // options.setTempRoot(options.getTempRoot() + "/temp-it");
    // options.setOutput(options.getTempRoot() + OUTPUT_FILE);
    options.setOutput(
        FileSystems.matchNewResource(options.getTempRoot(), true)
            .resolve(
                String.format("TfIdfIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
                StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("output", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("results", StandardResolveOptions.RESOLVE_FILE)
            .toString());
    options.setOnSuccessMatcher(
        new FileChecksumMatcher(EXPECTED_OUTPUT_CHECKSUM, options.getOutput() + "*-of-*"));
    Pipeline pipeline = Pipeline.create(options);
    pipeline.getCoderRegistry().registerCoderForClass(URI.class, StringDelegateCoder.of(URI.class));

    pipeline
        .apply(new ReadDocuments(TfIdf.listInputDocuments(options)))
        .apply(new ComputeTfIdf())
        .apply(new WriteTfIdf(options.getOutput()));

    pipeline.run().waitUntilFinish();
  }
}
