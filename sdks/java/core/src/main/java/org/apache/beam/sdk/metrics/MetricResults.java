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
  public MetricResult<Long> getCounter(MetricName name, @Nullable String scope) {
    MetricFilter.Builder filter = MetricFilter.builder().addName(name);
    if (scope != null) {
      filter.addStep(scope);
    }
    MetricQueryResults metrics = queryMetrics(filter.build());
    try {
      return Iterables.getOnlyElement(metrics.counters());
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Expected one matching counter", e);
    }
  }

  /**
   * Query for all metrics that match the filter.
   */
  public abstract MetricQueryResults queryMetrics(MetricFilter filter);
}
