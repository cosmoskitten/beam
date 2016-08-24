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

package org.apache.beam.runners.spark.aggregators.metrics;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.Map;
import java.util.SortedMap;

/**
 * A {@link MetricRegistry} decorator-like* that supports {@link AggregatorMetric} by exposing
 * the underlying * {@link org.apache.beam.runners.spark.aggregators.NamedAggregators}'
 * aggregators as {@link Gauge}s.
 * <p>
 * *{@link MetricRegistry} is not an interface, so this is not a by-the-book decorator.
 * That said, it delegates all metric related getters to the "decorated" instance.
 * </p>
 */
public class WithNamedAggregatorsSupport extends MetricRegistry {

  private MetricRegistry internalMetricRegistry;

  private WithNamedAggregatorsSupport(final MetricRegistry internalMetricRegistry) {
    this.internalMetricRegistry = internalMetricRegistry;
  }

  public static WithNamedAggregatorsSupport forRegistry(final MetricRegistry
                                                            metricRegistry) {
    return new WithNamedAggregatorsSupport(metricRegistry);
  }

  @Override
  public SortedMap<String, Timer> getTimers(MetricFilter filter) {
    return internalMetricRegistry.getTimers(filter);
  }

  @Override
  public SortedMap<String, Meter> getMeters(MetricFilter filter) {
    return internalMetricRegistry.getMeters(filter);
  }

  @Override
  public SortedMap<String, Histogram> getHistograms(MetricFilter filter) {
    return internalMetricRegistry.getHistograms(filter);
  }

  @Override
  public SortedMap<String, Counter> getCounters(final MetricFilter filter) {
    return internalMetricRegistry.getCounters(filter);
  }

  @Override
  public SortedMap<String, Gauge> getGauges(final MetricFilter filter) {
    return
        new ImmutableSortedMap.Builder<String, Gauge>(
            Ordering.from(String.CASE_INSENSITIVE_ORDER))
            .putAll(internalMetricRegistry.getGauges(filter))
            .putAll(extractGauges(internalMetricRegistry, filter))
            .build();
  }

  private Map<String, Gauge> transformToGauges(final AggregatorMetric metric) {
    return Maps.transformValues(metric.getNamedAggregators().renderAll(),
        new Function<Object, Gauge>() {

          @Override
          public Gauge apply(final Object rawGaugeObj) {
            return new Gauge<Double>() {

              @Override
              public Double getValue() {
                // at the moment the metric's type is assumed to be
                // compatible with Double. While far from perfect, it seems reasonable at
                // this point in time
                return Double.parseDouble(rawGaugeObj.toString());
              }
            };
          }
        });
  }

  private Map<String, Gauge> extractGauges(final MetricRegistry metricRegistry,
                                           final MetricFilter filter) {
    // find the AggregatorMetric metrics from within all currently registered metrics
    final Optional<Map.Entry<String, Metric>> aggregatorMetricEntryOp =
        FluentIterable
            .from(metricRegistry.getMetrics().entrySet())
            .firstMatch(new Predicate<Map.Entry<String, Metric>>() {

              @Override
              public boolean apply(Map.Entry<String, Metric> metricEntry) {
                return (metricEntry.getValue() instanceof AggregatorMetric)
                    && (filter.matches(metricEntry.getKey(), metricEntry.getValue()));
              }
            });

    return aggregatorMetricEntryOp.isPresent()
        ? transformToGauges((AggregatorMetric) aggregatorMetricEntryOp.get().getValue()) :
        ImmutableSortedMap.<String, Gauge>of();
  }
}
