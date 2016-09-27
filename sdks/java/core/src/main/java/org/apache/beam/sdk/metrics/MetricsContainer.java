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
import java.util.concurrent.atomic.AtomicBoolean;
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
 *   <li>Optionally, use {@link #getDeltas()} to get only the changes to a metric since the last
 *   time and report those periodically. After reporting deltas and before the next call to
 *   {@link #getDeltas()} the runner should invoke {@link #commitDeltas} to indicate that those
 *   deltas have been incorporated.
 *   </li>
 * </ol>
 */
@Experimental(Kind.METRICS)
public class MetricsContainer {

  private static final ThreadLocal<MetricsContainer> CONTAINER_FOR_THREAD =
      new ThreadLocal<MetricsContainer>();
  private final AtomicBoolean committedFirstDelta = new AtomicBoolean(false);

  private final String stepName;

  private MetricsMap<MetricName, CounterCell> counters =
      new MetricsMap<MetricName, CounterCell>() {
        @Override
        protected CounterCell createInstance() {
          return new CounterCell();
        }
      };

  private MetricsMap<MetricName, DistributionCell> distributions =
      new MetricsMap<MetricName, DistributionCell>() {
        @Override
        protected DistributionCell createInstance() {
          return new DistributionCell();
        }
      };

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
      MetricsMap<MetricName, CellT> cells,
      boolean includeZero) {
    ImmutableList.Builder<MetricUpdate<UpdateT>> updates = ImmutableList.builder();
    for (Map.Entry<MetricName, CellT> cell : cells.entries()) {
      UpdateT update = cell.getValue().getDeltaUpdate(includeZero);
      if (update != null) {
        updates.add(MetricUpdate.create(MetricKey.create(stepName, cell.getKey()), update));
      }
    }
    return updates.build();
  }

  /**
   * Return the delta updates for all metrics in this container.
   */
  public MetricUpdates getDeltas() {
    boolean includeZero = !committedFirstDelta.get();
    return MetricUpdates.create(
        extractUpdates(counters, includeZero),
        extractUpdates(distributions, includeZero));
  }

  private <UpdateT, CellT extends MetricCell<UpdateT>>
  void commitDeltas(MetricsMap<MetricName, CellT> cells, Iterable<MetricUpdate<UpdateT>> updates) {
    for (MetricUpdate<UpdateT> counterUpdate : updates) {
      CellT cell = checkNotNull(cells.get(counterUpdate.getKey().metricName()),
          "Metric cell %s in delta being committed should have already been defined.");
      cell.commitDeltaUpdate(counterUpdate.getUpdate());
    }
  }

  /**
   * Record all of the given deltas as reported.
   *
   * <p>The updates in {@code deltas} must have been produced by calling {@link #getDeltas()} on
   * this metrics container, and no deltas should have been committed since the associated call to
   * {@link #getDeltas()}.
   */
  public void commitDeltas(MetricUpdates deltas) {
    commitDeltas(counters, deltas.counterUpdates());
    commitDeltas(distributions, deltas.distributionUpdates());

    committedFirstDelta.set(true);
  }

  private <UpdateT, CellT extends MetricCell<UpdateT>>
  ImmutableList<MetricUpdate<UpdateT>> extractCumulatives(
      MetricsMap<MetricName, CellT> cells) {
    ImmutableList.Builder<MetricUpdate<UpdateT>> updates = ImmutableList.builder();
    for (Map.Entry<MetricName, CellT> cell : cells.entries()) {
      UpdateT update = checkNotNull(cell.getValue().getCumulativeUpdate());
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
          MetricKey.create(stepName, counter.getKey()), counter.getValue().getCumulativeUpdate()));
    }

    ImmutableList.Builder<MetricUpdate<DistributionData>> distributionUpdates =
        ImmutableList.builder();
    for (Map.Entry<MetricName, DistributionCell> distribution : distributions.entries()) {
      distributionUpdates.add(MetricUpdate.create(
          MetricKey.create(stepName, distribution.getKey()),
          distribution.getValue().getCumulativeUpdate()));
    }
    return MetricUpdates.create(
        extractCumulatives(counters),
        extractCumulatives(distributions));
  }
}
