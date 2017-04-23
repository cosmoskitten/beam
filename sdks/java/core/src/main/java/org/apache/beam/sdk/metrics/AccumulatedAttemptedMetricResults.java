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
package org.apache.beam.sdk.metrics;

import com.google.common.base.Function;

/**
 * Implementation of attempted {@link MetricResults} using accumulated {@link MetricsContainers}.
 */
public class AccumulatedAttemptedMetricResults extends AccumulatedMetricResults {

  public AccumulatedAttemptedMetricResults(MetricsContainers attemptedMetricsContainers) {
    super(attemptedMetricsContainers, new MetricsContainers());
  }

  @Override
  protected Function<AttemptedAndCommitted<Long>, MetricResult<Long>>
  counterUpdateToResult() {
    return new
        Function<AttemptedAndCommitted<Long>, MetricResult<Long>>() {
          @Override
          public MetricResult<Long>
          apply(AttemptedAndCommitted<Long> metricResult) {
            MetricKey key = metricResult.getKey();
            return new AccumulatedAttemptedMetricResult<>(
                key.metricName(),
                key.stepName(),
                metricResult.getAttempted().getUpdate());
          }
        };
  }

  @Override
  protected Function<AttemptedAndCommitted<DistributionData>, MetricResult<DistributionResult>>
  distributionUpdateToResult() {
    return new
        Function<AttemptedAndCommitted<DistributionData>, MetricResult<DistributionResult>>() {
          @Override
          public MetricResult<DistributionResult>
          apply(AttemptedAndCommitted<DistributionData> metricResult) {
            MetricKey key = metricResult.getKey();
            return new AccumulatedAttemptedMetricResult<>(
                key.metricName(),
                key.stepName(),
                metricResult.getAttempted().getUpdate().extractResult());
          }
        };
  }

  @Override
  protected Function<AttemptedAndCommitted<GaugeData>, MetricResult<GaugeResult>>
  gaugeUpdateToResult() {
    return new
        Function<AttemptedAndCommitted<GaugeData>, MetricResult<GaugeResult>>() {
          @Override
          public MetricResult<GaugeResult>
          apply(AttemptedAndCommitted<GaugeData> metricResult) {
            MetricKey key = metricResult.getKey();
            return new AccumulatedAttemptedMetricResult<>(
                key.metricName(),
                key.stepName(),
                metricResult.getAttempted().getUpdate().extractResult());
          }
        };
  }

  private static class AccumulatedAttemptedMetricResult<T> implements MetricResult<T> {
    private final MetricName name;
    private final String step;
    private final T result;

    AccumulatedAttemptedMetricResult(MetricName name, String step, T result) {
      this.name = name;
      this.step = step;
      this.result = result;
    }

    @Override
    public MetricName name() {
      return name;
    }

    @Override
    public String step() {
      return step;
    }

    @Override
    public T committed() {
      throw new UnsupportedOperationException("This runner does not currently support committed"
          + " metrics results. Please use 'attempted' instead.");
    }

    @Override
    public T attempted() {
      return result;
    }
  }
}
