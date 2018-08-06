package org.apache.beam.sdk.io.kinesis;

import java.util.function.BooleanSupplier;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.util.MovingFunction;
import org.joda.time.Duration;
import org.joda.time.Instant;

class KinesisWatermark {
  /** Period of updates to determine watermark. */
  private static final Duration SAMPLE_UPDATE = Duration.standardSeconds(5);

  /** Period of samples to determine watermark. */
  static final Duration SAMPLE_PERIOD = Duration.standardMinutes(1);

  /** Period after which watermark should be updated regardless of number of samples. */
  static final Duration UPDATE_THRESHOLD = SAMPLE_PERIOD.multipliedBy(2);

  /** Constant representing the maximum Kinesis stream retention period. */
  static final Duration MAX_KINESIS_STREAM_RETENTION_PERIOD = Duration.standardDays(7);

  /** Minimum number of unread messages required before considering updating watermark. */
  static final int MIN_MESSAGES = 10;

  /**
   * Minimum number of SAMPLE_UPDATE periods over which unread messages should be spread before
   * considering updating watermark.
   */
  private static final int MIN_SPREAD = 2;

  private Instant lastWatermark = Instant.now().minus(MAX_KINESIS_STREAM_RETENTION_PERIOD);
  private Instant lastUpdate = new Instant(0L);
  private final MovingFunction minReadTimestampMsSinceEpoch =
      new MovingFunction(
          SAMPLE_PERIOD.getMillis(),
          SAMPLE_UPDATE.getMillis(),
          MIN_SPREAD,
          MIN_MESSAGES,
          Min.ofLongs());

  public Instant getCurrent(BooleanSupplier shardsUpToDate) {
    Instant now = Instant.now();
    Instant readMin = getMinReadTimestamp(now);
    if (readMin == null) {
      if (shardsUpToDate.getAsBoolean()) {
        updateLastWatermark(now, now);
      }
    } else if (shouldUpdate(now)) {
      updateLastWatermark(readMin, now);
    }
    return lastWatermark;
  }

  public void update(Instant recordArrivalTime) {
    minReadTimestampMsSinceEpoch.add(Instant.now().getMillis(), recordArrivalTime.getMillis());
  }

  private Instant getMinReadTimestamp(Instant now) {
    long readMin = minReadTimestampMsSinceEpoch.get(now.getMillis());
    if (readMin == Min.ofLongs().identity()) {
      return null;
    } else {
      return new Instant(readMin);
    }
  }

  private boolean shouldUpdate(Instant now) {
    boolean hasEnoughSamples = minReadTimestampMsSinceEpoch.isSignificant();
    boolean isStale = lastUpdate.isBefore(now.minus(UPDATE_THRESHOLD));
    return hasEnoughSamples || isStale;
  }

  private void updateLastWatermark(Instant newWatermark, Instant now) {
    if (newWatermark.isAfter(lastWatermark)) {
      lastWatermark = newWatermark;
      lastUpdate = now;
    }
  }
}
