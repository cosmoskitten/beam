package org.apache.beam.runners.spark.aggregators.metrics.sink;

import org.apache.beam.runners.spark.aggregators.metrics.WithNamedAggregatorsSupport;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

import org.apache.spark.metrics.sink.Sink;


import java.util.Properties;


/**
 * An in-memory {@link Sink} implementation for tests.
 */
public class InMemoryMetrics implements Sink {

  private static WithNamedAggregatorsSupport extendedMetricsRegistry;
  private static MetricRegistry internalMetricRegistry;

  public InMemoryMetrics(final Properties properties,
                         final MetricRegistry metricRegistry,
                         final org.apache.spark.SecurityManager securityMgr) {
    extendedMetricsRegistry = WithNamedAggregatorsSupport.forRegistry(metricRegistry);
    internalMetricRegistry = metricRegistry;
  }

  @SuppressWarnings("unchecked")
  public static <T> T valueOf(final String name) {
    T retVal;

    if (extendedMetricsRegistry != null
      && extendedMetricsRegistry.getGauges().containsKey(name)) {
      retVal = (T) extendedMetricsRegistry.getGauges().get(name).getValue();
    } else {
      retVal = null;
    }

    return retVal;
  }

  public static void clearAll() {
    if (internalMetricRegistry != null) {
      internalMetricRegistry.removeMatching(MetricFilter.ALL);
    }
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  @Override
  public void report() {

  }

}
