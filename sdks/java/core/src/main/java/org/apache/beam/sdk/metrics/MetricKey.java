package org.apache.beam.sdk.metrics;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * Metrics are keyed by the step name they are associated with and the name of the metric.
 */
@Experimental(Kind.METRICS)
@AutoValue
public abstract class MetricKey {

  /** The step name that is associated with this metric. */
  public abstract String stepName();

  /** The name of the metric. */
  public abstract MetricName metricName();

  public static MetricKey create(String stepName, MetricName metricName) {
    return new AutoValue_MetricKey(stepName, metricName);
  }

  public static MetricKey create(String stepName, String namespace, String metricName) {
    return new AutoValue_MetricKey(stepName, MetricName.named(namespace, metricName));
  }
}
