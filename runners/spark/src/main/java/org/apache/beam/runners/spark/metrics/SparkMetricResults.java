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

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.runners.spark.metrics.MetricAggregator.CounterAggregator;
import org.apache.beam.runners.spark.metrics.MetricAggregator.DistributionAggregator;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.spark.Accumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link MetricResults} for the Spark Runner.
 */
public class SparkMetricResults extends MetricResults {
  private static final Logger LOG = LoggerFactory.getLogger(SparkMetricResults.class);

  private final Accumulator<SparkMetricsContainer> metricsAccum;

  public SparkMetricResults(Accumulator<SparkMetricsContainer> metricsAccum) {
    this.metricsAccum = metricsAccum;
  }

  @Override
  public MetricQueryResults queryMetrics(MetricsFilter filter) {
    return new SparkMetricQueryResults(filter);
  }

  private class SparkMetricQueryResults implements MetricQueryResults {
    private final MetricsFilter filter;

    SparkMetricQueryResults(MetricsFilter filter) {
      this.filter = filter;
    }

    @Override
    public Iterable<MetricResult<Long>> counters() {
      Iterable<CounterAggregator> allCounters =
          metricsAccum.value().getCounters();
      Iterable<CounterAggregator> filteredCounters = Iterables.filter(allCounters,
          new Predicate<CounterAggregator>() {
            @SuppressWarnings("ConstantConditions")
            @Override
            public boolean apply(CounterAggregator metricResult) {
              return matches(filter, metricResult.getKey());
            }
          });
      return Iterables.transform(filteredCounters,
          new Function<CounterAggregator, MetricResult<Long>>() {
            @SuppressWarnings("ConstantConditions")
            @Nullable
            @Override
            public MetricResult<Long> apply(CounterAggregator metricResult) {
              MetricKey key = metricResult.getKey();
              return new SparkMetricResult<>(key.metricName(), key.stepName(),
                  metricResult.getValue());
            }
          });
    }

    @Override
    public Iterable<MetricResult<DistributionResult>> distributions() {
      Iterable<DistributionAggregator> allDistributions =
          metricsAccum.value().getDistributions();
      Iterable<DistributionAggregator> filteredDistributions =
          Iterables.filter(allDistributions,
              new Predicate<DistributionAggregator>() {
            @SuppressWarnings("ConstantConditions")
            @Override
            public boolean apply(DistributionAggregator metricResult) {
              return matches(filter, metricResult.getKey());
            }
          });
      return Iterables.transform(filteredDistributions,
          new Function<DistributionAggregator, MetricResult<DistributionResult>>() {
            @SuppressWarnings("ConstantConditions")
            @Nullable
            @Override
            public MetricResult<DistributionResult>
            apply(DistributionAggregator metricResult) {
              MetricKey key = metricResult.getKey();
              return new SparkMetricResult<>(key.metricName(), key.stepName(),
                  metricResult.getValue().extractResult());
            }
          });
    }

    private boolean matches(MetricsFilter filter, MetricKey key) {
      return matchesName(key.metricName(), filter.names())
          && matchesScope(key.stepName(), filter.steps());
    }

    private boolean matchesName(MetricName metricName, Set<MetricNameFilter> nameFilters) {
      if (nameFilters.isEmpty()) {
        return true;
      }

      for (MetricNameFilter nameFilter : nameFilters) {
        if ((nameFilter.getName() == null || nameFilter.getName().equals(metricName.name()))
            && Objects.equal(metricName.namespace(), nameFilter.getNamespace())) {
          return true;
        }
      }

      return false;
    }

    private boolean matchesScope(String actualScope, Set<String> scopes) {
      if (scopes.isEmpty() || scopes.contains(actualScope)) {
        return true;
      }

      for (String scope : scopes) {
        if (actualScope.startsWith(scope)) {
          return true;
        }
      }

      return false;
    }
  }

  private static class SparkMetricResult<T> implements MetricResult<T> {
    private final MetricName name;
    private final String step;
    private final T result;

    SparkMetricResult(MetricName name, String step, T result) {
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
      return result;
    }

    @Override
    public T attempted() {
      return result;
    }
  }
}
