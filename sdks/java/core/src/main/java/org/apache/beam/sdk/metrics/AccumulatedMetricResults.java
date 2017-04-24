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
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.metrics.MetricUpdates.MetricUpdate;

/**
 * Implementation of {@link MetricResults} by means of accumulated {@link MetricsContainers}.
 */
public class AccumulatedMetricResults extends MetricResults {

  private final Map<MetricKey, AttemptedAndCommitted<Long>> counters = new HashMap<>();
  private final Map<MetricKey, AttemptedAndCommitted<DistributionData>> distributions =
      new HashMap<>();
  private final Map<MetricKey, AttemptedAndCommitted<GaugeData>> gauges = new HashMap<>();

  public AccumulatedMetricResults(
      MetricsContainers attemptedMetricsContainers,
      MetricsContainers committedMetricsContainers) {
    for (MetricsContainer container : attemptedMetricsContainers.asMap().values()) {
      MetricUpdates cumulative = container.getCumulative();
      mergeCounters(counters, cumulative.counterUpdates(), true);
      mergeDistributions(distributions, cumulative.distributionUpdates(), true);
      mergeGauges(gauges, cumulative.gaugeUpdates(), true);
    }
    for (MetricsContainer container : committedMetricsContainers.asMap().values()) {
      MetricUpdates cumulative = container.getCumulative();
      mergeCounters(counters, cumulative.counterUpdates(), false);
      mergeDistributions(distributions, cumulative.distributionUpdates(), false);
      mergeGauges(gauges, cumulative.gaugeUpdates(), false);
    }
  }

  @Override
  public MetricQueryResults queryMetrics(MetricsFilter filter) {
    return new QueryResults(filter);
  }

  private class QueryResults implements MetricQueryResults {
    private final MetricsFilter filter;

    private QueryResults(MetricsFilter filter) {
      this.filter = filter;
    }

    @Override
    public Iterable<MetricResult<Long>> counters() {
      return
          FluentIterable
              .from(counters.values())
              .filter(matchesFilter(filter))
              .transform(counterUpdateToResult())
              .toList();
    }

    @Override
    public Iterable<MetricResult<DistributionResult>> distributions() {
      return
          FluentIterable
              .from(distributions.values())
              .filter(matchesFilter(filter))
              .transform(distributionUpdateToResult())
              .toList();
    }

    @Override
    public Iterable<MetricResult<GaugeResult>> gauges() {
      return
          FluentIterable
              .from(gauges.values())
              .filter(matchesFilter(filter))
              .transform(gaugeUpdateToResult())
              .toList();
    }

    private Predicate<AttemptedAndCommitted<?>> matchesFilter(final MetricsFilter filter) {
      return new Predicate<AttemptedAndCommitted<?>>() {
        @Override
        public boolean apply(AttemptedAndCommitted<?> attemptedAndCommitted) {
          return MetricFiltering.matches(filter, attemptedAndCommitted.getKey());
        }
      };
    }
  }

  protected Function<AttemptedAndCommitted<Long>, MetricResult<Long>>
  counterUpdateToResult() {
    return new
        Function<AttemptedAndCommitted<Long>, MetricResult<Long>>() {
          @Override
          public MetricResult<Long>
          apply(AttemptedAndCommitted<Long> metricResult) {
            MetricKey key = metricResult.getKey();
            return new AccumulatedMetricResult<>(
                key.metricName(),
                key.stepName(),
                metricResult.getAttempted().getUpdate(),
                metricResult.getCommitted().getUpdate());
          }
        };
  }

  protected Function<AttemptedAndCommitted<DistributionData>, MetricResult<DistributionResult>>
  distributionUpdateToResult() {
    return new
        Function<AttemptedAndCommitted<DistributionData>, MetricResult<DistributionResult>>() {
          @Override
          public MetricResult<DistributionResult>
          apply(AttemptedAndCommitted<DistributionData> metricResult) {
            MetricKey key = metricResult.getKey();
            return new AccumulatedMetricResult<>(
                key.metricName(),
                key.stepName(),
                metricResult.getAttempted().getUpdate().extractResult(),
                metricResult.getCommitted().getUpdate().extractResult());
          }
        };
  }

  protected Function<AttemptedAndCommitted<GaugeData>, MetricResult<GaugeResult>>
  gaugeUpdateToResult() {
    return new
        Function<AttemptedAndCommitted<GaugeData>, MetricResult<GaugeResult>>() {
          @Override
          public MetricResult<GaugeResult>
          apply(AttemptedAndCommitted<GaugeData> metricResult) {
            MetricKey key = metricResult.getKey();
            return new AccumulatedMetricResult<>(
                key.metricName(),
                key.stepName(),
                metricResult.getAttempted().getUpdate().extractResult(),
                metricResult.getCommitted().getUpdate().extractResult());
          }
        };
  }

  private void mergeCounters(
      Map<MetricKey, AttemptedAndCommitted<Long>> counters,
      Iterable<MetricUpdate<Long>> updates,
      boolean isAttempted) {
    for (MetricUpdate<Long> update : updates) {
      MetricKey key = update.getKey();
      AttemptedAndCommitted<Long> updated;
      if (isAttempted) {
        updated = new AttemptedAndCommitted<>(
            key,
            update,
            MetricUpdate.create(key, 0L));
      } else {
        updated = new AttemptedAndCommitted<>(
            key,
            MetricUpdate.create(key, 0L),
            update);
      }
      if (counters.containsKey(key)) {
        AttemptedAndCommitted<Long> current = counters.get(key);
        updated = new AttemptedAndCommitted<>(
            key,
            MetricUpdate.create(
                key,
                updated.getAttempted().getUpdate() + current.getAttempted().getUpdate()),
            MetricUpdate.create(
                key,
                updated.getCommitted().getUpdate() + current.getCommitted().getUpdate()));
      }
      counters.put(key, updated);
    }
  }

  private void mergeDistributions(
      Map<MetricKey, AttemptedAndCommitted<DistributionData>> distributions,
      Iterable<MetricUpdate<DistributionData>> updates,
      boolean isAttempted) {
    for (MetricUpdate<DistributionData> update : updates) {
      MetricKey key = update.getKey();
      AttemptedAndCommitted<DistributionData> updated;
      if (isAttempted) {
        updated = new AttemptedAndCommitted<>(
            key,
            update,
            MetricUpdate.create(key, DistributionData.EMPTY));
      } else {
        updated = new AttemptedAndCommitted<>(
            key,
            MetricUpdate.create(key, DistributionData.EMPTY),
            update);
      }
      if (distributions.containsKey(key)) {
        AttemptedAndCommitted<DistributionData> current = distributions.get(key);
        updated = new AttemptedAndCommitted<>(
            key,
            MetricUpdate.create(
                key,
                updated.getAttempted().getUpdate().combine(current.getAttempted().getUpdate())),
            MetricUpdate.create(
                key,
                updated.getCommitted().getUpdate().combine(current.getCommitted().getUpdate())));
      }
      distributions.put(key, updated);
    }
  }


  private void mergeGauges(
      Map<MetricKey, AttemptedAndCommitted<GaugeData>> gauges,
      Iterable<MetricUpdate<GaugeData>> updates,
      boolean isAttempted) {
    for (MetricUpdate<GaugeData> update : updates) {
      MetricKey key = update.getKey();
      AttemptedAndCommitted<GaugeData> updated;
      if (isAttempted) {
        updated = new AttemptedAndCommitted<>(
            key,
            update,
            MetricUpdate.create(key, GaugeData.empty()));
      } else {
        updated = new AttemptedAndCommitted<>(
            key,
            MetricUpdate.create(key, GaugeData.empty()),
            update);
      }
      if (gauges.containsKey(key)) {
        AttemptedAndCommitted<GaugeData> current = gauges.get(key);
        updated = new AttemptedAndCommitted<>(
            key,
            MetricUpdate.create(
                key,
                updated.getAttempted().getUpdate().combine(current.getAttempted().getUpdate())),
            MetricUpdate.create(
                key,
                updated.getCommitted().getUpdate().combine(current.getCommitted().getUpdate())));
      }
      gauges.put(key, updated);
    }
  }

  /**
   * Accumulated implementation of {@link MetricResult}.
   */
  protected static class AccumulatedMetricResult<T> implements MetricResult<T> {
    private final MetricName name;
    private final String step;
    private final T attempted;
    private final T committed;

    public AccumulatedMetricResult(
        MetricName name,
        String step,
        T attempted,
        T committed) {
      this.name = name;
      this.step = step;
      this.attempted = attempted;
      this.committed = committed;
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
      return committed;
    }

    @Override
    public T attempted() {
      return attempted;
    }
  }

  /**
   * Attempted and committed {@link MetricUpdate MetricUpdates}.
   */
  protected static class AttemptedAndCommitted<T> {
    private final MetricKey key;
    private final MetricUpdate<T> attempted;
    private final MetricUpdate<T> committed;

    private AttemptedAndCommitted(MetricKey key, MetricUpdate<T> attempted,
        MetricUpdate<T> committed) {
      this.key = key;
      this.attempted = attempted;
      this.committed = committed;
    }

    protected MetricKey getKey() {
      return key;
    }

    protected MetricUpdate<T> getAttempted() {
      return attempted;
    }

    protected MetricUpdate<T> getCommitted() {
      return committed;
    }
  }
}
