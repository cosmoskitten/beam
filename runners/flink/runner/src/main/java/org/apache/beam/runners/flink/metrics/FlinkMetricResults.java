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
package org.apache.beam.runners.flink.metrics;

import static org.apache.beam.runners.flink.metrics.DoFnRunnerWithMetricsUpdate.COUNTER_PREFIX;
import static org.apache.beam.runners.flink.metrics.DoFnRunnerWithMetricsUpdate.DISTRIBUTION_PREFIX;
import static org.apache.beam.runners.flink.metrics.DoFnRunnerWithMetricsUpdate.parseMetricKey;

import com.google.common.base.Objects;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.metrics.DistributionData;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;

/**
 * Implementation of {@link MetricResults} for the Flink Runner.
 */
public class FlinkMetricResults extends MetricResults {

  private Map<String, Object> aggregators;

  public FlinkMetricResults(Map<String, Object> aggregators) {
    this.aggregators = aggregators;
  }

  @Override
  public MetricQueryResults queryMetrics(MetricsFilter filter) {
    return new FlinkMetricQueryResults(filter);
  }

  private class FlinkMetricQueryResults implements MetricQueryResults {

    private MetricsFilter filter;

    FlinkMetricQueryResults(MetricsFilter filter) {
      this.filter = filter;
    }

    @Override
    public Iterable<MetricResult<Long>> counters() {
      List<MetricResult<Long>> result = new ArrayList<>();
      for (Map.Entry<String, Object> entry : aggregators.entrySet()) {
        if (entry.getKey().startsWith(COUNTER_PREFIX)) {
          MetricKey metricKey = parseMetricKey(entry.getKey());
          if (matches(filter, metricKey)) {
            result.add(new FlinkMetricResult<>(
                metricKey.metricName(), metricKey.stepName(), (Long) entry.getValue()));
          }
        }
      }
      return result;
    }

    @Override
    public Iterable<MetricResult<DistributionResult>> distributions() {
      List<MetricResult<DistributionResult>> result = new ArrayList<>();
      for (Map.Entry<String, Object> entry : aggregators.entrySet()) {
        if (entry.getKey().startsWith(DISTRIBUTION_PREFIX)) {
          MetricKey metricKey = parseMetricKey(entry.getKey());
          DistributionData data = (DistributionData) entry.getValue();
          if (matches(filter, metricKey)) {
            result.add(new FlinkMetricResult<>(
                metricKey.metricName(), metricKey.stepName(), data.extractResult()));
          }
        }
      }
      return result;
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

  private static class FlinkMetricResult<T> implements MetricResult<T> {
    private final MetricName name;
    private final String step;
    private final T result;

    FlinkMetricResult(MetricName name, String step, T result) {
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
      throw new UnsupportedOperationException("Flink runner does not currently support committed"
          + " metrics results. Please use 'attempted' instead.");
    }

    @Override
    public T attempted() {
      return result;
    }
  }

}
