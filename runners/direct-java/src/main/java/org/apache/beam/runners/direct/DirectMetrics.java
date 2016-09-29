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
package org.apache.beam.runners.direct;

import com.google.auto.value.AutoValue;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.metrics.DistributionData;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricFilter;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricUpdates;
import org.apache.beam.sdk.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.MetricsMap;

/**
 * Implementation of {@link MetricResults} for the Direct Runner.
 */
class DirectMetrics extends MetricResults {

  private interface DirectMetric<UpdateT, ResultT> {
    void applyPhysical(UpdateT update);
    void applyLogical(UpdateT update);
    ResultT extractPhysical();
    ResultT extractLogical();
  }

  private static class DirectCounter implements DirectMetric<Long, Long> {
    private final AtomicLong physicalValue = new AtomicLong();
    private final AtomicLong logicalValue = new AtomicLong();

    @Override
    public void applyPhysical(Long update) {
      physicalValue.addAndGet(update);
    }

    @Override
    public Long extractPhysical() {
      return physicalValue.get();
    }

    @Override
    public void applyLogical(Long update) {
      logicalValue.addAndGet(update);
    }

    @Override
    public Long extractLogical() {
      return logicalValue.get();
    }
  }

  private static class DirectDistribution
      implements DirectMetric<DistributionData, DistributionResult> {
    private final AtomicReference<DistributionData> physicalValue =
        new AtomicReference(DistributionData.ZERO);
    private final AtomicReference<DistributionData> logicalValue =
        new AtomicReference(DistributionData.ZERO);

    @Override
    public void applyPhysical(DistributionData update) {
      DistributionData previous;
      do {
        previous = physicalValue.get();
      } while (!physicalValue.compareAndSet(previous, previous.add(update)));
    }

    @Override
    public DistributionResult extractPhysical() {
      return physicalValue.get().extractResult();
    }

    @Override
    public void applyLogical(DistributionData update) {
      DistributionData previous;
      do {
        previous = logicalValue.get();
      } while (!logicalValue.compareAndSet(previous, previous.add(update)));
    }

    @Override
    public DistributionResult extractLogical() {
      return logicalValue.get().extractResult();
    }
  }

  /** The current values of counters in memory. */
  private MetricsMap<MetricKey, DirectMetric<Long, Long>> counters =
      new MetricsMap<>(new MetricsMap.Factory<MetricKey, DirectMetric<Long, Long>>() {
        @Override
        public DirectMetric<Long, Long> createInstance(MetricKey unusedKey) {
          return new DirectCounter();
        }
      });
  private MetricsMap<MetricKey, DirectMetric<DistributionData, DistributionResult>> distributions =
      new MetricsMap<>(
          new MetricsMap.Factory<MetricKey, DirectMetric<DistributionData, DistributionResult>>() {
        @Override
        public DirectMetric<DistributionData, DistributionResult> createInstance(
            MetricKey unusedKey) {
          return new DirectDistribution();
        }
      });

  @AutoValue
  abstract static class DirectMetricQueryResults implements MetricQueryResults {
    public static MetricQueryResults create(
        Iterable<MetricResult<Long>> counters,
        Iterable<MetricResult<DistributionResult>> distributions) {
      return new AutoValue_DirectMetrics_DirectMetricQueryResults(counters, distributions);
    }
  }

  @AutoValue
  abstract static class DirectMetricResult<T> implements MetricResult<T> {
    public static <T> MetricResult<T> create(MetricName name, String scope,
        T committed, T attempted) {
      return new AutoValue_DirectMetrics_DirectMetricResult<T>(
          name, scope, committed, attempted);
    }
  }

  @Override
  public MetricQueryResults queryMetrics(MetricFilter filter) {
    ImmutableList.Builder<MetricResult<Long>> counterResults = ImmutableList.builder();
    for (Entry<MetricKey, DirectMetric<Long, Long>> counter : counters.entries()) {
      maybeExtractResult(filter, counterResults, counter);
    }
    ImmutableList.Builder<MetricResult<DistributionResult>> distributionResults =
        ImmutableList.builder();
    for (Entry<MetricKey, DirectMetric<DistributionData, DistributionResult>> distribution
        : distributions.entries()) {
      maybeExtractResult(filter, distributionResults, distribution);
    }

    return DirectMetricQueryResults.create(counterResults.build(), distributionResults.build());
  }

  private <ResultT> void maybeExtractResult(
      MetricFilter filter,
      ImmutableList.Builder<MetricResult<ResultT>> resultsBuilder,
      Map.Entry<MetricKey, ? extends DirectMetric<?, ResultT>> entry) {
    if (matches(filter, entry.getKey())) {
      resultsBuilder.add(DirectMetricResult.create(
          entry.getKey().metricName(),
          entry.getKey().stepName(),
          entry.getValue().extractLogical(),
          entry.getValue().extractPhysical()));
    }
  }

  private boolean matches(MetricFilter filter, MetricKey key) {
    return matchesName(key.metricName(), filter.names())
        && matchesScope(key.stepName(), filter.scopes());
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

  private boolean matchesName(MetricName metricName, Set<MetricName> names) {
    if (names.isEmpty() || names.contains(metricName)) {
      return true;
    }

    for (MetricName candidate : names) {
      if (Strings.isNullOrEmpty(candidate.getName())
          && Objects.equal(metricName.getNamespace(), candidate.getNamespace())) {
        return true;
      }
    }

    return false;
  }

  /** Apply metric updates that represent physical counter deltas to the current metric values. */
  public void applyPhysical(MetricUpdates updates) {
    for (MetricUpdate<Long> counter : updates.counterUpdates()) {
      counters.getOrCreate(counter.getKey()).applyPhysical(counter.getUpdate());
    }
    for (MetricUpdate<DistributionData> distribution : updates.distributionUpdates()) {
      distributions.getOrCreate(distribution.getKey()).applyPhysical(distribution.getUpdate());
    }
  }

  /** Apply metric updates that represent new logical values from a bundle being committed. */
  public void applyLogical(MetricUpdates updates) {
    for (MetricUpdate<Long> counter : updates.counterUpdates()) {
      counters.getOrCreate(counter.getKey()).applyLogical(counter.getUpdate());
    }
    for (MetricUpdate<DistributionData> distribution : updates.distributionUpdates()) {
      distributions.getOrCreate(distribution.getKey()).applyLogical(distribution.getUpdate());
    }
  }
}
