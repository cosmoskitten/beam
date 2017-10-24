package org.apache.beam.sdk.io.gcp.spanner;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;

public class RateLimiters {

  public static RateLimiter.Factory doNothing() {
    return NullFactory.INSTANCE;
  }

  public static RateLimiter.Factory newRateLimiter(long estimate, long cap, double delta) {
    return new SimpleFactory(estimate, cap, delta);
  }

  private enum NullFactory implements RateLimiter.Factory {
    INSTANCE {
      @Override public RateLimiter create() {
        return NullRateLimiter.INSTANCE;
      }
    };

  }

  private enum NullRateLimiter implements RateLimiter {
    INSTANCE;

    @Override public void start() {
      // Do nothing
    }

    @Override public void end() {
      // Do nothing
    }
  }

  private static class SimpleFactory implements RateLimiter.Factory {

    private final long estimate;
    private final long cap;
    private final double delta;

    public SimpleFactory(long estimate, long cap, double delta) {
      this.estimate = estimate;
      this.cap = cap;
      this.delta = delta;
    }

    @Override public RateLimiter create() {
      return new SimpleRateLimiter(estimate, cap, delta);
    }
  }

  private static class SimpleRateLimiter implements RateLimiter {

    private long estimate;
    private final long cap;
    private final double delta;

    private Stopwatch stopwatch;

    public SimpleRateLimiter(long value, long cap, double delta) {
      this.estimate = value;
      this.cap = cap;
      this.delta = delta;
    }

    @Override
    public void start() {
      Preconditions.checkState(stopwatch == null);

      stopwatch = Stopwatch.createStarted();
    }

    @Override
    public void end() {
      stopwatch.stop();

      long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
      long diff = elapsed - estimate;

      if (diff > 0) {
        try {
          Thread.sleep(diff);
        } catch (InterruptedException e) {
          // Do nothing
        }
      }

      estimate += delta * diff;

      estimate = Math.min(estimate, cap);

      stopwatch = null;
    }
  }


}
