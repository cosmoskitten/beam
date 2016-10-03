package org.apache.beam.sdk.metrics;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * The name of a metric consists of a {@link #namespace} and a {@link #name}. The {@link #namespace}
 * allows grouping related metrics together and also prevents collisions between multiple metrics
 * with the same name.
 */
@Experimental(Kind.METRICS)
@AutoValue
public abstract class MetricName {

  /** The namespace associated with this metric. */
  public abstract String namespace();

  /** The name of this metric. */
  public abstract String name();

  public static MetricName named(String namespace, String name) {
    return new AutoValue_MetricName(namespace, name);
  }

  public static MetricName named(Class<?> namespace, String name) {
    return new AutoValue_MetricName(namespace.getSimpleName(), name);
  }
}
