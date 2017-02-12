package org.apache.beam.sdk.transforms.windowing;

import com.google.auto.value.AutoValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** An abstract description of a standardized transformation on timestamps. */
public abstract class TimestampTransform {

  /** Returns a transform that shifts a timestamp later by {@code delay}. */
  public static TimestampTransform delay(Duration delay) {
    return new AutoValue_TimestampTransform_Delay(delay);
  }

  /**
   * Returns a transform that aligns a timestamp to the next boundary of {@code period}, starting
   * from {@code offset}.
   */
  public static TimestampTransform alignTo(Duration period, Instant offset) {
    return new AutoValue_TimestampTransform_AlignTo(period, offset);
  }

  /**
   * Returns a transform that aligns a timestamp to the next boundary of {@code period}, starting
   * from the start of the epoch.
   */
  public static TimestampTransform alignTo(Duration period) {
    return alignTo(period, new Instant(0));
  }

  /**
   * Represents the transform that aligns a timestamp to the next boundary of {@link #getPeriod()}
   * start at {@link #getOffset()}.
   */
  @AutoValue
  public abstract static class AlignTo extends TimestampTransform {
    public abstract Duration getPeriod();

    public abstract Instant getOffset();
  }

  /** Represents the transform that delays a timestamp by {@link #getDelay()}. */
  @AutoValue
  public abstract static class Delay extends TimestampTransform {
    public abstract Duration getDelay();
  }
}
