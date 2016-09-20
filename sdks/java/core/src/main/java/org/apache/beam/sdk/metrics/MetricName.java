package org.apache.beam.sdk.metrics;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * The name of a metric.
 */
@Experimental(Kind.METRICS)
@AutoValue
public abstract class MetricName {

  /** The namespace that declared this metric. */
  public abstract String getNamespace();

  /**
   * The name of this metric. Should only be null in {@link MetricFilter MetricFilters} being used
   * to query metrics, in which case it indicates that all metrics in the given namespace should
   * be matched.
   */
  @Nullable
  public abstract String getName();

  public static MetricName named(String namespace, String name) {
    return new AutoValue_MetricName(namespace, name);
  }

  public static MetricName named(Class<?> namespace, String name) {
    return new AutoValue_MetricName(namespace.getSimpleName(), name);
  }
}
