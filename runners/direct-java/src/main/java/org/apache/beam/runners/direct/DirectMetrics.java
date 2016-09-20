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

  private interface DirectMetric<T> {
    void applyPhysical(MetricUpdate<T> update);
    void applyLogical(MetricUpdate<T> update);
    T extractPhysical();
    T extractLogical();
  }

  private static class DirectCounter implements DirectMetric<Long> {
    private final AtomicLong physicalValue = new AtomicLong();
    private final AtomicLong logicalValue = new AtomicLong();

    @Override
    public void applyPhysical(MetricUpdate<Long> update) {
      physicalValue.addAndGet(update.getUpdate());
    }

    @Override
    public Long extractPhysical() {
      return physicalValue.get();
    }

    @Override
    public void applyLogical(MetricUpdate<Long> update) {
      logicalValue.addAndGet(update.getUpdate());
    }

    @Override
    public Long extractLogical() {
      return logicalValue.get();
    }
  }

  /** The current values of counters in memory. */
  private MetricsMap<MetricKey, DirectMetric<Long>> counters =
      new MetricsMap<MetricKey, DirectMetric<Long>>() {
        @Override
        protected DirectMetric<Long> createInstance() {
          return new DirectCounter();
        }
      };

  @AutoValue
  abstract static class DirectMetricQueryResults implements MetricQueryResults {
    public static MetricQueryResults create(Iterable<MetricResult<Long>> counters) {
      return new AutoValue_DirectMetrics_DirectMetricQueryResults(counters);
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
    for (Entry<MetricKey, DirectMetric<Long>> counter : counters.entries()) {
      maybeExtractResult(filter, counterResults, counter);
    }

    return DirectMetricQueryResults.create(counterResults.build());
  }

  private <T> void maybeExtractResult(
      MetricFilter filter,
      ImmutableList.Builder<MetricResult<T>> resultsBuilder,
      Map.Entry<MetricKey, ? extends DirectMetric<T>> entry) {
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
      counters.getOrCreate(counter.getKey()).applyPhysical(counter);
    }
  }

  /** Apply metric updates that represent new logical values from a bundle being committed. */
  public void applyLogical(MetricUpdates updates) {
    for (MetricUpdate<Long> counter : updates.counterUpdates()) {
      counters.getOrCreate(counter.getKey()).applyLogical(counter);
    }
  }
}
