package org.apache.beam.sdk.metrics;

import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * Tracks the current value (and delta) for a Counter metric for a specific context and bundle.
 */
@Experimental(Kind.METRICS)
class CounterCell implements MetricCell<Long> {

  private final AtomicLong value = new AtomicLong();
  private final AtomicLong delta = new AtomicLong();

  /** Increment the counter by the given amount. */
  public void add(long n) {
    delta.addAndGet(n);
    value.addAndGet(n);
  }

  @Nullable
  @Override
  public Long getDeltaUpdate(boolean includeZero) {
    long delta = this.delta.get();
    return (delta == 0 && !includeZero) ? null : delta;
  }

  @Override
  public void commitDeltaUpdate(Long deltaUpdate) {
    delta.addAndGet(-1 * deltaUpdate);
  }

  @Override
  public Long getCumulativeUpdate() {
    return value.get();
  }
}
