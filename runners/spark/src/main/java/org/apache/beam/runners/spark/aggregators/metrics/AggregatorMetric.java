package org.apache.beam.runners.spark.aggregators.metrics;

import org.apache.beam.runners.spark.aggregators.NamedAggregators;

import com.codahale.metrics.Metric;


/**
 * An adapter between the {@link NamedAggregators} and codahale's {@link Metric}
 * interface.
 */
public class AggregatorMetric implements Metric {

  private final NamedAggregators namedAggregators;

  private AggregatorMetric(final NamedAggregators namedAggregators) {
    this.namedAggregators = namedAggregators;
  }

  public static AggregatorMetric of(final NamedAggregators namedAggregators) {
    return new AggregatorMetric(namedAggregators);
  }

  NamedAggregators getNamedAggregators() {
    return namedAggregators;
  }
}
