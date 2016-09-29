package org.apache.beam.sdk.metrics;

import autovalue.shaded.com.google.common.common.collect.Iterables;
import javax.annotation.Nullable;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * Methods for interacting with the metrics of a pipeline that has been executed. Accessed via
 * {@link PipelineResult#metrics()}.
 */
@Experimental(Kind.METRICS)
public abstract class MetricResults {

  /** Retrieve the current counter value. */
  public MetricResult<Long> getCounter(MetricNameFilter name, @Nullable String step) {
    // TODO: Move these default implementations into a base class that may optionally be used.
    // Some runners may support more efficient mechanisms for querying a specific metric, so
    // wouldn't want to rely on "query for metrics then find the match".
    MetricsFilter.Builder filter = MetricsFilter.builder().addNameFilter(name);
    if (step != null) {
      filter.addStep(step);
    }
    MetricQueryResults metrics = queryMetrics(filter.build());
    try {
      return Iterables.getOnlyElement(metrics.counters());
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Expected one matching counter", e);
    }
  }

  /** Retrieve the current distribution value. */
  public MetricResult<DistributionResult> getDistribution(
      MetricNameFilter name, @Nullable String step) {
    MetricsFilter.Builder filter = MetricsFilter.builder().addNameFilter(name);
    if (step != null) {
      filter.addStep(step);
    }
    MetricQueryResults metrics = queryMetrics(filter.build());
    try {
      return Iterables.getOnlyElement(metrics.distributions());
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Expected one matching distribution", e);
    }
  }

  /**
   * Query for all metrics that match the filter.
   */
  public abstract MetricQueryResults queryMetrics(MetricsFilter filter);
}
