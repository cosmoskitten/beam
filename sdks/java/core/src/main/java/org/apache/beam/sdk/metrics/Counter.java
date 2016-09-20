package org.apache.beam.sdk.metrics;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * A metric that reports a single long value and can be incremented or decremented.
 */
@Experimental(Kind.METRICS)
public class Counter {

  private final MetricName name;

  Counter(MetricName name) {
    this.name = name;
  }

  /** Increment the counter. */
  public void inc() {
    inc(1);
  }

  /** Increment the counter by the given amount. */
  public void inc(long n) {
    MetricsContainer.getCurrentContainer().getOrCreateCounter(name).add(n);
  }

  /* Decrement the counter. */
  public void dec() {
    dec(1);
  }

  /* Decrement the counter by the given amount. */
  public void dec(long n) {
    MetricsContainer.getCurrentContainer().getOrCreateCounter(name).add(-1 * n);
  }
}
