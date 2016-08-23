package org.apache.beam.runners.spark.aggregators.metrics;

import org.apache.beam.runners.spark.aggregators.NamedAggregators;

import com.codahale.metrics.MetricRegistry;

import org.apache.spark.metrics.source.Source;

/**
 * A Spark {@link Source} that is tailored to expose an {@link AggregatorMetric},
 * wrapping an underlying {@link NamedAggregators} instance.
 */
public class AggregatorMetricSource implements Source {

  private static final String SOURCE_NAME = "NamedAggregators";

  private final MetricRegistry metricRegistry = new MetricRegistry();

  public AggregatorMetricSource(final NamedAggregators aggregators) {
    metricRegistry.register(SOURCE_NAME, AggregatorMetric.of(aggregators));
  }

  @Override
  public String sourceName() {
    return SOURCE_NAME;
  }

  @Override
  public MetricRegistry metricRegistry() {
    return metricRegistry;
  }
}
