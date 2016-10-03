package org.apache.beam.sdk.metrics;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * Tracks the current value (and delta) for a Counter metric for a specific context and bundle.
 */
@Experimental(Kind.METRICS)
class CounterCell extends MetricCell<Long> {

  private final AtomicLong value = new AtomicLong();

  /** Increment the counter by the given amount. */
  public void add(long n) {
    value.addAndGet(n);
    markDirtyAfterModification();
  }

  @Override
  public Long getCumulative() {
    return value.get();
  }
}
