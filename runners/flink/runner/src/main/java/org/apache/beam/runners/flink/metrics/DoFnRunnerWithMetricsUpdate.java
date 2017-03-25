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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.sdk.metrics.DistributionData;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricUpdates;
import org.apache.beam.sdk.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.joda.time.Instant;

/**
 * DoFnRunner decorator which registers {@link org.apache.beam.sdk.metrics.MetricsContainer}.
 * It update metrics to Flink metric and accumulator in finishBundle.
 */
public class DoFnRunnerWithMetricsUpdate<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {

  private static final String METRIC_KEY_SEPARATOR = "__";
  static final String COUNTER_PREFIX = "__counter";
  static final String DISTRIBUTION_PREFIX = "__distribution";

  private MetricsContainer metricsContainer;
  private final String stepName;
  private final DoFnRunner<InputT, OutputT> delegate;
  private final RuntimeContext runtimeContext;
  private final Map<String, Counter> counterCache;
  private final Map<String, DistributionGauge> distributionGaugeCache;

  public DoFnRunnerWithMetricsUpdate(
      String stepName,
      DoFnRunner<InputT, OutputT> delegate,
      RuntimeContext runtimeContext) {
    metricsContainer = new MetricsContainer(stepName);
    this.stepName = stepName;
    this.delegate = delegate;
    this.runtimeContext = runtimeContext;
    counterCache = new HashMap<>();
    distributionGaugeCache = new HashMap<>();
  }

  @Override
  public void startBundle() {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
      delegate.startBundle();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void processElement(final WindowedValue<InputT> elem) {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
      delegate.processElement(elem);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onTimer(final String timerId, final BoundedWindow window, final Instant timestamp,
                      final TimeDomain timeDomain) {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
      delegate.onTimer(timerId, window, timestamp, timeDomain);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void finishBundle() {
    try (Closeable ignored = MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
      delegate.finishBundle();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // update metrics
    MetricUpdates updates = metricsContainer.getUpdates();
    if (updates != null) {
      updateCounters(updates.counterUpdates());
      updateDistributions(updates.distributionUpdates());
      metricsContainer.commitUpdates();
    }

  }

  private void updateCounters(Iterable<MetricUpdate<Long>> updates) {

    for (MetricUpdate<Long> metricUpdate : updates) {

      String flinkMetricName = getFlinkMetricNameString(COUNTER_PREFIX, metricUpdate.getKey());
      Long update = metricUpdate.getUpdate();

      // update flink metric
      Counter counter = counterCache.get(flinkMetricName);
      if (counter == null) {
        counter = runtimeContext.getMetricGroup().counter(flinkMetricName);
        counterCache.put(flinkMetricName, counter);
      }
      counter.dec(counter.getCount());
      counter.inc(update);

      // update flink accumulator
      Accumulator<Long, Long> accumulator = runtimeContext.getAccumulator(flinkMetricName);
      if (accumulator == null) {
        accumulator = new LongCounter();
        runtimeContext.addAccumulator(flinkMetricName, accumulator);
      }
      accumulator.resetLocal();
      accumulator.add(update);
    }
  }

  private void updateDistributions(Iterable<MetricUpdate<DistributionData>> updates) {

    for (MetricUpdate<DistributionData> metricUpdate : updates) {

      String flinkMetricName =
          getFlinkMetricNameString(DISTRIBUTION_PREFIX, metricUpdate.getKey());
      DistributionData update = metricUpdate.getUpdate();

      // update flink metric
      DistributionGauge gauge = distributionGaugeCache.get(flinkMetricName);
      if (gauge == null) {
        gauge = runtimeContext.getMetricGroup()
            .gauge(flinkMetricName, new DistributionGauge(update));
        distributionGaugeCache.put(flinkMetricName, gauge);
      } else {
        gauge.update(update);
      }

      // update flink accumulator
      Accumulator<DistributionData, DistributionData> accumulator =
          runtimeContext.getAccumulator(flinkMetricName);
      if (accumulator == null) {
        accumulator = new AccumulatorForDistributionData();
        runtimeContext.addAccumulator(flinkMetricName, accumulator);
      }
      accumulator.resetLocal();
      accumulator.add(update);
    }
  }

  private static String getFlinkMetricNameString(String prefix, MetricKey key) {
    return prefix
        + METRIC_KEY_SEPARATOR + key.stepName()
        + METRIC_KEY_SEPARATOR + key.metricName().namespace()
        + METRIC_KEY_SEPARATOR + key.metricName().name();
  }

  static MetricKey parseMetricKey(String flinkMetricName) {
    String[] arr = flinkMetricName.split(METRIC_KEY_SEPARATOR);
    return MetricKey.create(arr[2], MetricName.named(arr[3], arr[4]));
  }

  /**
   * {@link Gauge} for {@link DistributionData}.
   */
  public static class DistributionGauge implements Gauge<DistributionData> {

    DistributionData data;

    DistributionGauge(DistributionData data) {
      this.data = data;
    }

    void update(DistributionData newData) {
      data = newData;
    }

    @Override
    public DistributionData getValue() {
      return data;
    }
  }

}
