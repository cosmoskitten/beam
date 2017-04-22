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
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.metrics.DistributionData;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeData;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricFiltering;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricUpdates;
import org.apache.beam.sdk.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsContainers;
import org.apache.beam.sdk.metrics.MetricsFilter;


/**
 * Implementation of {@link MetricResults} for the Spark Runner.
 */
public class SparkMetricResults extends MetricResults {

  private final Map<MetricKey, MetricUpdate<Long>> counters = new HashMap<>();
  private final Map<MetricKey, MetricUpdates.MetricUpdate<DistributionData>> distributions =
      new HashMap<>();
  private final Map<MetricKey, MetricUpdates.MetricUpdate<GaugeData>> gauges = new HashMap<>();

  public SparkMetricResults(MetricsContainers metricsContainers) {
    for (MetricsContainer container : metricsContainers.asMap().values()) {
      MetricUpdates cumulative = container.getCumulative();
      mergeCounters(cumulative.counterUpdates());
      mergeDistributions(cumulative.distributionUpdates());
      mergeGauges(cumulative.gaugeUpdates());
    }
  }

  @Override
  public MetricQueryResults queryMetrics(MetricsFilter filter) {

    return new QueryResults(filter, counters.values(), distributions.values(), gauges.values());
  }

  private void mergeCounters(
      Iterable<MetricUpdates.MetricUpdate<Long>> updates) {
    for (MetricUpdates.MetricUpdate<Long> update : updates) {
      MetricKey key = update.getKey();
      MetricUpdates.MetricUpdate<Long> current = counters.get(key);
      counters.put(key, current != null
          ? MetricUpdates.MetricUpdate.create(key, current.getUpdate() + update.getUpdate())
          : update);
    }
  }

  private void mergeDistributions(
      Iterable<MetricUpdates.MetricUpdate<DistributionData>> updates) {
    for (MetricUpdates.MetricUpdate<DistributionData> update : updates) {
      MetricKey key = update.getKey();
      MetricUpdates.MetricUpdate<DistributionData> current = distributions.get(key);
      distributions.put(key, current != null
          ? MetricUpdates.MetricUpdate.create(key, current.getUpdate().combine(update.getUpdate()))
          : update);
    }
  }

  private void mergeGauges(
      Iterable<MetricUpdates.MetricUpdate<GaugeData>> updates) {
    for (MetricUpdates.MetricUpdate<GaugeData> update : updates) {
      MetricKey key = update.getKey();
      MetricUpdates.MetricUpdate<GaugeData> current = gauges.get(key);
      gauges.put(
          key,
          current != null
              ? MetricUpdates.MetricUpdate.create(
              key,
              current.getUpdate().combine(update.getUpdate()))
              : update);
    }
  }

  private class QueryResults implements MetricQueryResults {
    private final MetricsFilter filter;

    public QueryResults(MetricsFilter filter, Iterable<MetricUpdates.MetricUpdate<Long>> counters,
        Iterable<MetricUpdates.MetricUpdate<DistributionData>> distributions,
        Iterable<MetricUpdates.MetricUpdate<GaugeData>> gauges) {
      this.filter = filter;
    }

    @Override
    public Iterable<MetricResult<Long>> counters() {
      return
          FluentIterable
              .from(counters.values())
              .filter(matchesFilter(filter))
              .transform(TO_COUNTER_RESULT)
              .toList();
    }

    @Override
    public Iterable<MetricResult<DistributionResult>> distributions() {
      return
          FluentIterable
              .from(distributions.values())
              .filter(matchesFilter(filter))
              .transform(TO_DISTRIBUTION_RESULT)
              .toList();
    }

    @Override
    public Iterable<MetricResult<GaugeResult>> gauges() {
      return
          FluentIterable
              .from(gauges.values())
              .filter(matchesFilter(filter))
              .transform(TO_GAUGE_RESULT)
              .toList();
    }

    private Predicate<MetricUpdates.MetricUpdate<?>> matchesFilter(final MetricsFilter filter) {
      return new Predicate<MetricUpdates.MetricUpdate<?>>() {
        @Override
        public boolean apply(MetricUpdates.MetricUpdate<?> metricResult) {
          return MetricFiltering.matches(filter, metricResult.getKey());
        }
      };
    }
  }

  private static final
  Function<MetricUpdates.MetricUpdate<DistributionData>, MetricResult<DistributionResult>>
      TO_DISTRIBUTION_RESULT =
      new Function<MetricUpdates.MetricUpdate<DistributionData>,
          MetricResult<DistributionResult>>() {
        @Override
        public MetricResult<DistributionResult>
        apply(MetricUpdates.MetricUpdate<DistributionData> metricResult) {
          if (metricResult != null) {
            MetricKey key = metricResult.getKey();
            return new AttemptedMetricResult<>(key.metricName(), key.stepName(),
                metricResult.getUpdate().extractResult());
          } else {
            return null;
          }
        }
      };

  private static final Function<MetricUpdates.MetricUpdate<Long>, MetricResult<Long>>
      TO_COUNTER_RESULT =
      new Function<MetricUpdates.MetricUpdate<Long>, MetricResult<Long>>() {
        @Override
        public MetricResult<Long> apply(MetricUpdates.MetricUpdate<Long> metricResult) {
          if (metricResult != null) {
            MetricKey key = metricResult.getKey();
            return new AttemptedMetricResult<>(key.metricName(), key.stepName(),
                metricResult.getUpdate());
          } else {
            return null;
          }
        }
      };

  private static final Function<MetricUpdates.MetricUpdate<GaugeData>, MetricResult<GaugeResult>>
      TO_GAUGE_RESULT =
      new Function<MetricUpdates.MetricUpdate<GaugeData>, MetricResult<GaugeResult>>() {
        @Override
        public MetricResult<GaugeResult> apply(MetricUpdates.MetricUpdate<GaugeData> metricResult) {
          if (metricResult != null) {
            MetricKey key = metricResult.getKey();
            return new AttemptedMetricResult<>(key.metricName(), key.stepName(),
                metricResult.getUpdate().extractResult());
          } else {
            return null;
          }
        }
      };

  private static class AttemptedMetricResult<T> implements MetricResult<T> {
    private final MetricName name;
    private final String step;
    private final T result;

    AttemptedMetricResult(MetricName name, String step, T result) {
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
