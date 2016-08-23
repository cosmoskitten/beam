package org.apache.beam.runners.spark;

import org.apache.beam.runners.spark.aggregators.metrics.sink.InMemoryMetrics;
import org.junit.rules.ExternalResource;

/**
 * A rule that cleans the {@link InMemoryMetrics} after the tests has finished.
 */
class InMemoryMetricsSinkRule extends ExternalResource {
  @Override
  protected void before() throws Throwable {
    InMemoryMetrics.clearAll();
  }
}
