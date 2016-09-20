package org.apache.beam.sdk.metrics;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * Methods for interacting with the metrics of a pipeline that has been executed. Accessed via
 * {@link PipelineResult#metrics()}.
 */
@Experimental(Kind.METRICS)
public abstract class MetricResults {
  /**
   * Query for all metrics that match the filter.
   */
  public abstract MetricQueryResults queryMetrics(MetricsFilter filter);
}
