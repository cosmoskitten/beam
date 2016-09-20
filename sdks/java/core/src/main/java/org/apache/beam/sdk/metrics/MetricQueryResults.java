package org.apache.beam.sdk.metrics;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * The results of a query for metrics. Allows accessing all of the metrics that matched the filter.
 */
@Experimental(Kind.METRICS)
public interface MetricQueryResults {
  /** Return the metric results for the counters that matched the filter. */
  Iterable<MetricResult<Long>> counters();
}
