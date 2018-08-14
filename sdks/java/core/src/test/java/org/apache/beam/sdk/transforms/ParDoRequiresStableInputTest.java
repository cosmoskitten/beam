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
package org.apache.beam.sdk.transforms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.testing.FileChecksumMatcher;
import org.apache.beam.sdk.testing.RetryFailures;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * ValidatesRunner test for the support of {@link
 * org.apache.beam.sdk.transforms.DoFn.RequiresStableInput} annotation.
 */
@RunWith(JUnit4.class)
public class ParDoRequiresStableInputTest {

  private static final String VALUE = "value";
  // SHA-1 hash of string "value"
  private static final String VALUE_CHECKSUM = "f32b67c7e26342af42efabc674d441dca0a281c5";

  private static class PairWithRandomKeyFn extends SimpleFunction<String, KV<String, String>> {
    @Override
    public KV<String, String> apply(String value) {
      String key = UUID.randomUUID().toString();
      return KV.of(key, value);
    }
  }

  private static class MakeSideEffectAndThenFailFn extends DoFn<KV<String, String>, String> {
    private final String outputPrefix;

    private MakeSideEffectAndThenFailFn(String outputPrefix) {
      this.outputPrefix = outputPrefix;
    }

    @RequiresStableInput
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      MatchResult matchResult = FileSystems.match(outputPrefix + "*");
      boolean firstTime = (matchResult.metadata().size() == 0);

      KV<String, String> kv = c.element();
      writeTextToFileSideEffect(kv.getValue(), outputPrefix + kv.getKey());
      if (firstTime) {
        throw new Exception("Deliberate failure: should happen only once.");
      }
    }

    private static void writeTextToFileSideEffect(String text, String filename) throws IOException {
      ResourceId rid = FileSystems.matchNewResource(filename, false);
      WritableByteChannel chan = FileSystems.create(rid, "text/plain");
      chan.write(ByteBuffer.wrap(text.getBytes(Charset.defaultCharset())));
      chan.close();
    }
  }

  private static void runRequiresStableInputPipeline(RequiresStableInputTestOptions options) {
    Pipeline p = Pipeline.create(options);

    PCollection<String> singleton = p.apply("CreatePCollectionOfOneValue", Create.of(VALUE));
    singleton
        .apply("Single-PairWithRandomKey", MapElements.via(new PairWithRandomKeyFn()))
        .apply(
            "Single-MakeSideEffectAndThenFail",
            ParDo.of(new MakeSideEffectAndThenFailFn(options.getParDoSingleOutputPrefix())));
    singleton
        .apply("Multi-PairWithRandomKey", MapElements.via(new PairWithRandomKeyFn()))
        .apply(
            "Multi-MakeSideEffectAndThenFail",
            ParDo.of(new MakeSideEffectAndThenFailFn(options.getParDoMultiOutputPrefix()))
                .withOutputTags(new TupleTag<>(), TupleTagList.empty()));

    p.run().waitUntilFinish();
  }

  @BeforeClass
  public static void setup() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  /**
   * Option for ParDoRequiresStableInputTest.
   *
   * <p>Define the output prefixes for tests on {@link ParDo.SingleOutput} and {@link
   * ParDo.MultiOutput}.
   */
  public interface RequiresStableInputTestOptions extends TestPipelineOptions {
    @Description("Prefix of the output files for testing ParDo.SingleOutput")
    @Required
    String getParDoSingleOutputPrefix();

    void setParDoSingleOutputPrefix(String value);

    @Description("Prefix of the output files for testing ParDo.MultiOutput")
    @Required
    String getParDoMultiOutputPrefix();

    void setParDoMultiOutputPrefix(String value);
  }

  @Test
  @Category({ValidatesRunner.class, RetryFailures.class})
  public void testParDoRequiresStableInput() {
    RequiresStableInputTestOptions options =
        TestPipeline.testingPipelineOptions().as(RequiresStableInputTestOptions.class);
    ResourceId dir =
        FileSystems.matchNewResource(options.getTempRoot(), true)
            .resolve(
                String.format("requires-stable-input-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
                StandardResolveOptions.RESOLVE_DIRECTORY);

    options.setParDoSingleOutputPrefix(
        dir.resolve("pardo-single-output", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("key-", StandardResolveOptions.RESOLVE_FILE)
            .toString());
    options.setParDoMultiOutputPrefix(
        dir.resolve("pardo-multi-output", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("key-", StandardResolveOptions.RESOLVE_FILE)
            .toString());
    options.setOnSuccessMatcher(
        new FileChecksumMatcher(VALUE_CHECKSUM, options.getParDoSingleOutputPrefix() + "*", false));
    options.setOnSuccessMatcher(
        new FileChecksumMatcher(VALUE_CHECKSUM, options.getParDoMultiOutputPrefix() + "*", false));

    runRequiresStableInputPipeline(options);
  }
}
