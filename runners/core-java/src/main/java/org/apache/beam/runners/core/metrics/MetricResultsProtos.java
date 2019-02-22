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
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.Timestamp;

/**
 * Convert between Java SDK {@link MetricResults} and corresponding protobuf {@link
 * BeamFnApi.MetricResults}.
 *
 * <p>Their structures are similar, but the former distinguishes "attempted" and "committed" values
 * at the lowest level (a {@link MetricResult} contains both), while the latter separates them into
 * two lists of {@link MonitoringInfo}s at the top level ({@link
 * BeamFnApi.MetricResults#getAttemptedList attempted}, {@link
 * BeamFnApi.MetricResults#getCommittedList} committed}.
 *
 * <p>THe proto form also holds more kinds of values, so some {@link MonitoringInfo}s may be dropped
 * from converting to the Java SDK form.
 */
public class MetricResultsProtos {

  /** Convert metric results from proto to Java SDK form. */
  public static MetricResults fromProto(BeamFnApi.MetricResults metricResults) {
    return new MetricResultsBuilder(metricResults).build();
  }

  /** Convert metric results from Java SDK to proto form. */
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

  // `toProto` helpers

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

  /**
   * Add a metric key and value (from a {@link MetricResult}) to a {@link
   * BeamFnApi.MetricResults.Builder}.
   *
   * @param builder proto {@link BeamFnApi.MetricResults.Builder} to add to
   * @param set set this metric's value on a {@link SimpleMonitoringInfoBuilder} (via an API that's
   *     specific to this metric's type, as provided by the caller)
   * @param add add the {@link MonitoringInfo} created from this key/value to either the "attempted"
   *     or "committed" list of the {@link BeamFnApi.MetricResults.Builder builder}
   */
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

  // `fromProto` helpers

  /**
   * Helper for converting {@link BeamFnApi.MetricResults} to {@link MetricResults}.
   *
   * <p>The former separates "attempted" and "committed" metrics at the highest level, while the
   * latter splits on metric-type (counter, distribution, or gauge) first, so converting basically
   * amounts pivoting those between those two.
   */
  private static class MetricResultsBuilder {
    private final Map<MetricKey, MetricResultBuilder<Long>> counters = new ConcurrentHashMap<>();
    private final Map<MetricKey, MetricResultBuilder<DistributionResult>> distributions =
        new ConcurrentHashMap<>();
    private final Map<MetricKey, MetricResultBuilder<GaugeResult>> gauges =
        new ConcurrentHashMap<>();

    /**
     * Populate metric-type-specific maps with {@link MonitoringInfo}s from a {@link
     * BeamFnApi.MetricResults}.
     */
    public MetricResultsBuilder(BeamFnApi.MetricResults metrics) {
      add(metrics.getAttemptedList(), false);
      add(metrics.getCommittedList(), true);
    }

    private void add(Iterable<MonitoringInfo> monitoringInfos, Boolean committed) {
      for (MonitoringInfo monitoringInfo : monitoringInfos) {
        add(monitoringInfo, committed);
      }
    }

    public MetricResults build() {
      return new DefaultMetricResults(build(counters), build(distributions), build(gauges));
    }

    /** Helper for turning a map of result-builders into a sequence of results. */
    private static <T> List<MetricResult<T>> build(Map<MetricKey, MetricResultBuilder<T>> map) {
      return map.values().stream().map(MetricResultBuilder::build).collect(toList());
    }

    private void add(MonitoringInfo monitoringInfo, Boolean committed) {
      add(
          keyFromMonitoringInfo(monitoringInfo),
          monitoringInfo.getType(),
          monitoringInfo.getTimestamp(),
          monitoringInfo.getMetric(),
          committed);
    }

    private void add(
        MetricKey metricKey, String type, Timestamp timestamp, Metric metric, Boolean committed) {
      processMetric(
          metric,
          type,
          timestamp,
          counter -> add(metricKey, counter, committed, counters),
          distribution -> add(metricKey, distribution, committed, distributions),
          gauge -> add(metricKey, gauge, committed, gauges));
    }

    private <T> void add(
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

    /**
     * Helper class for storing and combining {@link MetricResult#getAttempted attempted} and {@link
     * MetricResult#getCommitted committed} values, to obtain complete {@link MetricResult}s.
     *
     * <p>Used while traversing {@link MonitoringInfo} lists where they are stored separately.
     */
    private static class MetricResultBuilder<T> {
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
  }
}
