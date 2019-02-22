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
package org.apache.beam.runners.core.metrics;

import static java.util.stream.Collectors.toList;
import static org.apache.beam.runners.core.metrics.MonitoringInfos.keyFromMonitoringInfo;
import static org.apache.beam.runners.core.metrics.MonitoringInfos.processMetric;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Metric;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.construction.metrics.MetricKey;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;

/** Convert {@link MetricResults} to and from {@link BeamFnApi.MetricResults}. */
public class MetricResultsProtos {

  private static <T> void process(
      BeamFnApi.MetricResults.Builder builder,
      MetricKey metricKey,
      T value,
      BiConsumer<SimpleMonitoringInfoBuilder, T> set,
      BiConsumer<BeamFnApi.MetricResults.Builder, MonitoringInfo> add) {
    if (value != null) {
      SimpleMonitoringInfoBuilder partial =
          new SimpleMonitoringInfoBuilder().handleMetricKey(metricKey);
      ;
      set.accept(partial, value);
      MonitoringInfo monitoringInfo = partial.build();
      if (monitoringInfo != null) {
        add.accept(builder, monitoringInfo);
      }
    }
  }

  /**
   * Add this {@link MetricResult}'s "attempted" and "committed" values to the corresponding lists
   * of {@param builder}.
   */
  private static <T> void process(
      BeamFnApi.MetricResults.Builder builder,
      MetricResult<T> metricResult,
      BiConsumer<SimpleMonitoringInfoBuilder, T> set) {
    MetricKey metricKey = MetricKey.create(metricResult.getStep(), metricResult.getName());
    ;
    process(
        builder,
        metricKey,
        metricResult.getAttempted(),
        set,
        BeamFnApi.MetricResults.Builder::addAttempted);
    try {
      process(
          builder,
          metricKey,
          metricResult.getCommitted(),
          set,
          BeamFnApi.MetricResults.Builder::addCommitted);
    } catch (UnsupportedOperationException ignored) {
    }
  }

  // Convert between proto- and SDK-representations of MetricResults

  /** Convert a {@link MetricResults} to a {@link BeamFnApi.MetricResults}. */
  public static BeamFnApi.MetricResults toProto(MetricResults metricResults) {
    BeamFnApi.MetricResults.Builder builder = BeamFnApi.MetricResults.newBuilder();
    MetricQueryResults results = metricResults.queryMetrics(null);
    results
        .getCounters()
        .forEach(counter -> process(builder, counter, SimpleMonitoringInfoBuilder::setInt64Value));
    results
        .getDistributions()
        .forEach(
            distribution ->
                process(
                    builder, distribution, SimpleMonitoringInfoBuilder::setIntDistributionValue));
    results
        .getGauges()
        .forEach(gauge -> process(builder, gauge, SimpleMonitoringInfoBuilder::setGaugeValue));
    return builder.build();
  }

  /**
   * Helper for converting {@link BeamFnApi.MetricResults} to {@link MetricResults}.
   *
   * <p>The former separates "attempted" and "committed" metrics, while the latter splits on
   * metric-type (counter, distribution, or gauge) at the top level, so converting basically amounts
   * to performing that pivot.
   */
  private static class MetricResultsBuilder {
    public static class MetricResultBuilder<T> {
      private final MetricKey key;
      private final T attempted;
      @Nullable private T committed;

      public MetricResultBuilder(MetricKey key, T attempted) {
        this.key = key;
        this.attempted = attempted;
      }

      public void setCommitted(T committed) {
        this.committed = committed;
      }

      public MetricResult<T> build() {
        if (attempted == null) {
          throw new IllegalStateException(
              "Can't build MetricResult with null attemptd value (committed: " + committed + ")");
        }
        return MetricResult.create(key.metricName(), key.stepName(), committed, attempted);
      }
    }

    private final Map<MetricKey, MetricResultBuilder<Long>> counters = new ConcurrentHashMap<>();
    private final Map<MetricKey, MetricResultBuilder<DistributionResult>> distributions =
        new ConcurrentHashMap<>();
    private final Map<MetricKey, MetricResultBuilder<GaugeResult>> gauges =
        new ConcurrentHashMap<>();

    public MetricResultsBuilder(BeamFnApi.MetricResults metrics) {
      add(metrics.getAttemptedList(), false);
      add(metrics.getCommittedList(), true);
    }

    /** Helper for turning a map of result-builders into a sequence of results. */
    private static <T> List<MetricResult<T>> build(Map<MetricKey, MetricResultBuilder<T>> map) {
      return map.values().stream().map(MetricResultBuilder::build).collect(toList());
    }

    public MetricResults build() {
      return new DefaultMetricResults(build(counters), build(distributions), build(gauges));
    }

    public void add(Iterable<MonitoringInfo> monitoringInfos, Boolean committed) {
      for (MonitoringInfo monitoringInfo : monitoringInfos) {
        add(monitoringInfo, committed);
      }
    }

    public void add(MonitoringInfo monitoringInfo, Boolean committed) {
      add(
          keyFromMonitoringInfo(monitoringInfo),
          monitoringInfo.getType(),
          monitoringInfo.getMetric(),
          committed);
    }

    public void add(MetricKey metricKey, String type, Metric metric, Boolean committed) {
      processMetric(
          metric,
          type,
          counter -> add(metricKey, counter, committed, counters),
          distribution -> add(metricKey, distribution, committed, distributions),
          gauge -> add(metricKey, gauge, committed, gauges));
    }

    public <T> void add(
        MetricKey key, T value, Boolean committed, Map<MetricKey, MetricResultBuilder<T>> map) {
      if (committed) {
        MetricResultBuilder builder = map.get(key);
        if (builder == null) {
          throw new IllegalStateException(
              String.format("No attempted value found for committed metric %s: %s", key, value));
        }
        builder.setCommitted(value);
      } else {
        map.put(key, new MetricResultBuilder(key, value));
      }
    }
  }

  public static MetricResults fromProto(BeamFnApi.MetricResults metricResults) {
    return new MetricResultsBuilder(metricResults).build();
  }
}
