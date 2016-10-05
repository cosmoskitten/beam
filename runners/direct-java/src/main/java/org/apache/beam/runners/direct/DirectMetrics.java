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
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.metrics.DistributionData;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricUpdates;
import org.apache.beam.sdk.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.MetricsMap;

/**
 * Implementation of {@link MetricResults} for the Direct Runner.
 */
class DirectMetrics extends MetricResults {

  private static final ExecutorService COUNTER_COMMITTER = Executors.newCachedThreadPool();

  public abstract static class DirectMetric<UpdateT, ResultT> {
    private AtomicReference<UpdateT> finishedCommitted;

    private final Object attemptedLock = new Object();
    private volatile UpdateT finishedAttempted;
    private final ConcurrentMap<CommittedBundle<?>, UpdateT> inflightAttempted =
        new ConcurrentHashMap<>();

    public DirectMetric(UpdateT zero) {
      finishedCommitted = new AtomicReference<>(zero);
      finishedAttempted = zero;
    }

    protected abstract UpdateT combine(Iterable<UpdateT> updates);
    private UpdateT combine(UpdateT update1, UpdateT update2) {
      return combine(Arrays.asList(update1, update2));
    }

    protected abstract ResultT extract(UpdateT data);

    public void updatePhysical(CommittedBundle<?> bundle, UpdateT tentativeCumulative) {
      inflightAttempted.put(bundle, tentativeCumulative);
    }

    public void commitPhysical(final CommittedBundle<?> bundle, final UpdateT finalCumulative) {
      // To prevent a query from blocking the commit, we perform the commit in two steps.
      // 1. We perform a non-blocking write to the uncommitted table to make the new vaule
      //    available immediately.
      // 2. We submit a runnable that will commit the update and remove the tentative value in
      //    a synchronized block.
      inflightAttempted.put(bundle, finalCumulative);
      COUNTER_COMMITTER.submit(new Runnable() {
        @Override
        public void run() {
          synchronized (attemptedLock) {
            finishedAttempted = combine(Arrays.asList(finishedAttempted, finalCumulative));
            inflightAttempted.remove(bundle);
          }
        }
      });
    }

    /** Extract the latest values from all committed and in-progress bundles. */
    public ResultT extractLatestAttempted() {
      ArrayList<UpdateT> updates = new ArrayList<>(inflightAttempted.size() + 1);
      // Within this block we know that will be consistent. Specifically, the only change that can
      // happen concurrently is the addition of new (larger) values to inflightAttempted.
      synchronized (attemptedLock) {
        updates.add(finishedAttempted);
        updates.addAll(inflightAttempted.values());
      }
      return extract(combine(updates));
    }

    public void commitLogical(final CommittedBundle<?> bundle, final UpdateT finalCumulative) {
      UpdateT current;
      do {
        current = finishedCommitted.get();
      } while (!finishedCommitted.compareAndSet(current, combine(current, finalCumulative)));
    }

    /** Extract the from all successfully committed bundles. */
    public ResultT extractCommitted() {
      return extract(finishedCommitted.get());
    }
  }

  private static class DirectCounter extends DirectMetric<Long, Long> {

    public DirectCounter() {
      super(0L);
    }

    @Override
    protected Long combine(Iterable<Long> updates) {
      long value = 0;
      for (long update : updates) {
        value += update;
      }
      return value;
    }

    @Override
    protected Long extract(Long data) {
      return data;
    }
  }

  private static class DirectDistribution
      extends DirectMetric<DistributionData, DistributionResult> {
    private final AtomicReference<DistributionData> physicalValue =
        new AtomicReference(DistributionData.EMPTY);
    private final AtomicReference<DistributionData> logicalValue =
        new AtomicReference(DistributionData.EMPTY);

    public DirectDistribution() {
      super(DistributionData.EMPTY);
    }

    @Override
    protected DistributionData combine(Iterable<DistributionData> updates) {
      DistributionData result = DistributionData.EMPTY;
      for (DistributionData update : updates) {
        result = result.combine(update);
      }
      return result;
    }

    @Override
    protected DistributionResult extract(DistributionData data) {
      return data.extractResult();
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
  public MetricQueryResults queryMetrics(MetricsFilter filter) {
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
      MetricsFilter filter,
      ImmutableList.Builder<MetricResult<ResultT>> resultsBuilder,
      Map.Entry<MetricKey, ? extends DirectMetric<?, ResultT>> entry) {
    if (matches(filter, entry.getKey())) {
      resultsBuilder.add(DirectMetricResult.create(
          entry.getKey().metricName(),
          entry.getKey().stepName(),
          entry.getValue().extractCommitted(),
          entry.getValue().extractLatestAttempted()));
    }
  }

  private boolean matches(MetricsFilter filter, MetricKey key) {
    return matchesName(key.metricName(), filter.names())
        && matchesScope(key.stepName(), filter.steps());
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

  /** Apply metric updates that represent physical counter deltas to the current metric values. */
  public void updatePhysical(CommittedBundle<?> bundle, MetricUpdates updates) {
    for (MetricUpdate<Long> counter : updates.counterUpdates()) {
      counters.getOrCreate(counter.getKey()).updatePhysical(bundle, counter.getUpdate());
    }
    for (MetricUpdate<DistributionData> distribution : updates.distributionUpdates()) {
      distributions.getOrCreate(distribution.getKey())
          .updatePhysical(bundle, distribution.getUpdate());
    }
  }

  public void commitPhysical(CommittedBundle<?> bundle, MetricUpdates updates) {
    for (MetricUpdate<Long> counter : updates.counterUpdates()) {
      counters.getOrCreate(counter.getKey()).commitPhysical(bundle, counter.getUpdate());
    }
    for (MetricUpdate<DistributionData> distribution : updates.distributionUpdates()) {
      distributions.getOrCreate(distribution.getKey())
          .commitPhysical(bundle, distribution.getUpdate());
    }
  }

  /** Apply metric updates that represent new logical values from a bundle being committed. */
  public void commitLogical(CommittedBundle<?> bundle, MetricUpdates updates) {
    for (MetricUpdate<Long> counter : updates.counterUpdates()) {
      counters.getOrCreate(counter.getKey()).commitLogical(bundle, counter.getUpdate());
    }
    for (MetricUpdate<DistributionData> distribution : updates.distributionUpdates()) {
      distributions.getOrCreate(distribution.getKey())
          .commitLogical(bundle, distribution.getUpdate());
    }
  }
}
