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

package org.apache.beam.runners.spark.metrics;

import static org.apache.beam.sdk.metrics.MetricMatchers.metricResult;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.TestSparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.UsesMetrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * Test metrics support in Spark runner.
 */
public class SparkMetricsTest implements Serializable {

  @After
  public void tearDown() {
    MetricsEnvironment.setCurrentContainer(null);
  }

  @Category({RunnableOnService.class, UsesMetrics.class})
  @Test
  public void metricsReportToQuery() {
    final Counter count = Metrics.counter(SparkMetricsTest.class, "count");
    SparkPipelineOptions options = PipelineOptionsFactory.create().as(SparkPipelineOptions.class);
    options.setRunner(TestSparkRunner.class);
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(Create.of(5, 8, 13))
        .apply("MyStep1", ParDo.of(new DoFn<Integer, Integer>() {
          @SuppressWarnings("unused")
          @ProcessElement
          public void processElement(ProcessContext c) {
            Distribution values = Metrics.distribution(SparkMetricsTest.class, "input");
            count.inc();
            values.update(c.element());

            c.output(c.element());
            c.output(c.element());
          }
        }))
        .apply("MyStep2", ParDo.of(new DoFn<Integer, Integer>() {
          @SuppressWarnings("unused")
          @ProcessElement
          public void processElement(ProcessContext c) {
            Distribution values = Metrics.distribution(SparkMetricsTest.class, "input");
            count.inc();
            values.update(c.element());
          }
        }));
    PipelineResult result = pipeline.run();

    result.waitUntilFinish();

    MetricQueryResults metrics = result.metrics().queryMetrics(MetricsFilter.builder()
        .addNameFilter(MetricNameFilter.inNamespace(SparkMetricsTest.class))
        .build());
    // TODO: BEAM-1169: Metrics shouldn't verify the physical values tightly.
    assertThat(metrics.counters(), hasItem(
        metricResult(SparkMetricsTest.class.getName(), "count", "MyStep1", 3L, 3L)));
    assertThat(metrics.distributions(), hasItem(
        metricResult(SparkMetricsTest.class.getName(), "input", "MyStep1",
            DistributionResult.create(26L, 3L, 5L, 13L),
            DistributionResult.create(26L, 3L, 5L, 13L))));

    assertThat(metrics.counters(), hasItem(
        metricResult(SparkMetricsTest.class.getName(), "count", "MyStep2", 6L, 6L)));
    assertThat(metrics.distributions(), hasItem(
        metricResult(SparkMetricsTest.class.getName(), "input", "MyStep2",
            DistributionResult.create(52L, 6L, 5L, 13L),
            DistributionResult.create(52L, 6L, 5L, 13L))));
  }
}

