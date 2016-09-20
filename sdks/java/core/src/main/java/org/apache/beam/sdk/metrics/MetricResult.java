package org.apache.beam.sdk.metrics;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * The results of a single current metric.
 */
@Experimental(Kind.METRICS)
public interface MetricResult<T> {
  /** Return the name of the metric. */
  MetricName name();
  /** Return the step context to which this metric result applies. */
  String step();

  /**
   * Return the value of this metric across all successfully completed parts of the pipeline.
   *
   * <p>Not all runners will support committed metrics. If they are not supported, the runner will
   * throw an {@link UnsupportedOperationException}.
   */
  T committed();

  /**
   * Return the value of this metric across all attempts of executing all parts of the pipeline.
   */
  T attempted();
}
