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

package org.apache.beam.runners.spark;

import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import org.apache.beam.runners.spark.examples.WordCount;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.runners.spark.translation.streaming.utils.SparkTestPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.spark.api.java.JavaSparkContext;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Test {@link SparkNativePipelineVisitor} with different pipelines.
 */
public class SparkNativePipelineVisitorTest {

  private static final String[] WORDS = {
      "hi there", "hi", "hi sue bob",
      "hi sue", "", "bob hi"};

  private File outputDir;

  @Rule
  public final SparkTestPipelineOptions pipelineOptions = new SparkTestPipelineOptions();

  @Rule
  public final TemporaryFolder tmpDir = new TemporaryFolder();

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Before
  public void setUp() throws IOException {
    outputDir = tmpDir.newFolder("out");
    outputDir.delete();
  }

  @Test
  public void debugBoundedPipeline() {
    JavaSparkContext jsc = new JavaSparkContext("local[*]", "Existing_Context");

    SparkPipelineOptions options = getDebugOptions(jsc);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()))
        .apply(new WordCount.CountWords())
        .apply(MapElements.via(new WordCount.FormatAsTextFn()))
        .apply(TextIO.Write.to(outputDir.getAbsolutePath()).withNumShards(3).withSuffix(".txt"));

    TransformTranslator.Translator translator = new TransformTranslator.Translator();
    EvaluationContext context = new EvaluationContext(jsc, pipeline);
    SparkNativePipelineVisitor visitor = new SparkNativePipelineVisitor(translator, context);

    pipeline.traverseTopologically(visitor);

    final String expectedPipeline = "sparkContext.parallelize(Arrays.asList(???))\n"
        + ".mapPartitions(new org.apache.beam.runners.spark.examples.WordCount$ExtractWordsFn())\n"
        + ".mapPartitions(new org.apache.beam.sdk.transforms.Count$PerElement$1())\n"
        + ".<combinePerKey>\n"
        + ".mapPartitions(new org.apache.beam.runners.spark.examples.WordCount$FormatAsTextFn())\n."
        + "<org.apache.beam.sdk.io.TextIO$Write$Bound>";

    assertThat("Debug pipeline did not equal expected",
        visitor.getDebugString(),
        Matchers.equalTo(expectedPipeline));
  }

  private SparkPipelineOptions getDebugOptions(JavaSparkContext jsc) {
    SparkContextOptions options = PipelineOptionsFactory.as(SparkContextOptions.class);
    options.setProvidedSparkContext(jsc);
    options.setDebugPipeline(true);
    options.setRunner(TestSparkRunner.class);
    return options;
  }
}
