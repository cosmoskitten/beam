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

import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.SparkPipelineEvaluator;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.SparkProcessContext;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.runners.spark.translation.streaming.StreamingEvaluationContext;
import org.apache.beam.runners.spark.translation.streaming.StreamingTransformTranslator;
import org.apache.beam.runners.spark.translation.streaming.StreamingWindowPipelineDetector;
import org.apache.beam.runners.spark.util.SinglePrimitiveOutputPTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;

import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SparkPipelineRunner translate operations defined on a pipeline to a representation executable
 * by Spark, and then submitting the job to Spark to be executed. If we wanted to run a dataflow
 * pipeline with the default options of a single threaded spark instance in local mode, we would do
 * the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * EvaluationResult result = SparkPipelineRunner.create().run(p);
 * }
 *
 * To create a pipeline runner to run against a different spark cluster, with a custom master url we
 * would do the following:
 *
 * {@code
 * Pipeline p = [logic for pipeline creation]
 * SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
 * options.setSparkMaster("spark://host:port");
 * EvaluationResult result = SparkPipelineRunner.create(options).run(p);
 * }
 *
 * To create a Spark streaming pipeline runner use {@link SparkStreamingPipelineOptions}
 */
public final class TestSparkPipelineRunner extends PipelineRunner<EvaluationResult> {

  private SparkPipelineRunner delegate;

  private TestSparkPipelineRunner(SparkPipelineOptions options) {
    this.delegate = SparkPipelineRunner.fromOptions(options);
  }

  public static TestSparkPipelineRunner fromOptions(PipelineOptions options) {
    // Default options suffice to set it up as a test runner
    SparkPipelineOptions sparkOptions =
        PipelineOptionsValidator.validate(SparkPipelineOptions.class, options);
    return new TestSparkPipelineRunner(sparkOptions);
  }

  @Override
  public EvaluationResult run(Pipeline pipeline) {
    return delegate.run(pipeline);
  }
}