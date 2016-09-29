package org.apache.beam.sdk.metrics;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * The name of a metric.
 */
@Experimental(Kind.METRICS)
@AutoValue
public abstract class MetricName {

  /** The inNamespace that declared this metric. */
  public abstract String getNamespace();

  /**
   * The name of this metric.
   */
  public abstract String getName();

  public static MetricName named(String namespace, String name) {
    return new AutoValue_MetricName(namespace, name);
  }

  public static MetricName named(Class<?> namespace, String name) {
    return new AutoValue_MetricName(namespace.getSimpleName(), name);
  }
}
