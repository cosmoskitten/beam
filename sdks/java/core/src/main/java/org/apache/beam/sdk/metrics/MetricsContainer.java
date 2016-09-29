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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.metrics.MetricUpdates.MetricUpdate;

/**
 * Holds all of the metrics produced for a single step and unit-of-commit.
 *
 * <p>A thread-local variable holds the {@link MetricsContainer} that should be used for any
 * metric updates produced from the executing code by interactions with the user-facing metric
 * interfaces (eg., {@link Counter}).
 * *
 * <p>For a given runner to support metrics it is currently necessary to do 3 things:
 * <ol>
 *   <li>Create a {@link MetricsContainer} for each scope that metrics will be reported at.
 *   </li>
 *   <li>Make sure to call {@link #setMetricsContainer}
 *   </li>
 *   <li>Use {@link #getCumulative()} to get {@link org.apache.beam.sdk.metrics.MetricUpdates}
 *   representing all the metric changes and report/aggregate those appropriately.
 *   </li>
 *   <li>Optionally, use {@link #getUpdates()} to get only the changes to a metric since the last
 *   time and report those periodically. After reporting deltas and before the next call to
 *   {@link #getUpdates()} the runner should invoke {@link #commitUpdates} to indicate that those
 *   deltas have been incorporated.
 *   </li>
 * </ol>
 */
@Experimental(Kind.METRICS)
public class MetricsContainer {

  private static final ThreadLocal<MetricsContainer> CONTAINER_FOR_THREAD =
      new ThreadLocal<MetricsContainer>();

  private final String stepName;

  private MetricsMap<MetricName, CounterCell> counters =
      new MetricsMap<>(new MetricsMap.Factory<MetricName, CounterCell>() {
        @Override
        public CounterCell createInstance(MetricName unusedKey) {
          return new CounterCell();
        }
      });

  private MetricsMap<MetricName, DistributionCell> distributions =
      new MetricsMap<>(new MetricsMap.Factory<MetricName, DistributionCell>() {
        @Override
        public DistributionCell createInstance(MetricName unusedKey) {
          return new DistributionCell();
        }
      });

  /**
   * Create a new {@link MetricsContainer} associated with the given {@code stepName}.
   */
  public MetricsContainer(String stepName) {
    this.stepName = stepName;
  }

  /**
   * Set the {@link MetricsContainer} for the current thread.
   */
  public static void setMetricsContainer(MetricsContainer container) {
    CONTAINER_FOR_THREAD.set(container);
  }

  /**
   * Clear the {@link MetricsContainer} for the current thread.
   */
  public static void unsetMetricsContainer() {
    CONTAINER_FOR_THREAD.remove();
  }

  /**
   * Return the {@link MetricsContainer} for the current thread.
   */
  public static MetricsContainer getCurrentContainer() {
    MetricsContainer container = CONTAINER_FOR_THREAD.get();
    if (container == null) {
      throw new IllegalStateException("Must call setMetricsContainer before reporting metrics.");
    }
    return container;
  }

  /**
   * Return the {@link CounterCell} that should be used for implementing the given
   * {@code metricName} in this container.
   */
  public CounterCell getOrCreateCounter(MetricName metricName) {
    return counters.getOrCreate(metricName);
  }

  public DistributionCell getOrCreateDistribution(MetricName metricName) {
    return distributions.getOrCreate(metricName);
  }

  private <UpdateT, CellT extends MetricCell<UpdateT>>
  ImmutableList<MetricUpdate<UpdateT>> extractUpdates(
      MetricsMap<MetricName, CellT> cells) {
    ImmutableList.Builder<MetricUpdate<UpdateT>> updates = ImmutableList.builder();
    for (Map.Entry<MetricName, CellT> cell : cells.entries()) {
      UpdateT update = cell.getValue().getUpdateIfDirty();
      if (update != null) {
        updates.add(MetricUpdate.create(MetricKey.create(stepName, cell.getKey()), update));
      }
    }
    return updates.build();
  }

  /**
   * Return the cumulative values for any metrics that have changed since the last time updates were
   * committed.
   */
  public MetricUpdates getUpdates() {
    return MetricUpdates.create(
        extractUpdates(counters),
        extractUpdates(distributions));
  }

  private void commitUpdates(MetricsMap<MetricName, ? extends MetricCell<?>> cells) {
    for (MetricCell<?> cell : cells.values()) {
      cell.commitUpdate();
    }
  }

  /**
   * Mark all of the updates that were retrieved with the latest call to {@link #getUpdates()} as
   * committed.
   */
  public void commitUpdates() {
    commitUpdates(counters);
    commitUpdates(distributions);
  }

  private <UpdateT, CellT extends MetricCell<UpdateT>>
  ImmutableList<MetricUpdate<UpdateT>> extractCumulatives(
      MetricsMap<MetricName, CellT> cells) {
    ImmutableList.Builder<MetricUpdate<UpdateT>> updates = ImmutableList.builder();
    for (Map.Entry<MetricName, CellT> cell : cells.entries()) {
      UpdateT update = checkNotNull(cell.getValue().getCumulative());
      updates.add(MetricUpdate.create(MetricKey.create(stepName, cell.getKey()), update));
    }
    return updates.build();
  }

  /**
   * Return the {@link MetricUpdates} representing the cumulative values of all metrics in this
   * container.
   */
  public MetricUpdates getCumulative() {
    ImmutableList.Builder<MetricUpdate<Long>> counterUpdates = ImmutableList.builder();
    for (Map.Entry<MetricName, CounterCell> counter : counters.entries()) {
      counterUpdates.add(MetricUpdate.create(
          MetricKey.create(stepName, counter.getKey()), counter.getValue().getCumulative()));
    }

    ImmutableList.Builder<MetricUpdate<DistributionData>> distributionUpdates =
        ImmutableList.builder();
    for (Map.Entry<MetricName, DistributionCell> distribution : distributions.entries()) {
      distributionUpdates.add(MetricUpdate.create(
          MetricKey.create(stepName, distribution.getKey()),
          distribution.getValue().getCumulative()));
    }
    return MetricUpdates.create(
        extractCumulatives(counters),
        extractCumulatives(distributions));
  }
}
