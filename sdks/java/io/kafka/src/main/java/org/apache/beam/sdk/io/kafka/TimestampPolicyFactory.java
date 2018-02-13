package org.apache.beam.sdk.io.kafka;

import java.io.Serializable;
import java.util.Optional;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * TODO.
 */
public abstract class TimestampPolicyFactory<KeyT, ValueT> implements Serializable {

  public abstract TimestampPolicy<KeyT, ValueT> createTimestampPolicy(
    TopicPartition tp, Optional<Instant> previousWatermark);

  /**
   * TODO.
   */
  public static <K, V> TimestampPolicyFactory<K, V> withProcessingTime() {
    return new TimestampPolicyFactory<K, V>() {
      @Override
      public TimestampPolicy<K, V>
      createTimestampPolicy(TopicPartition tp, Optional<Instant> previousWatermark) {
        return new ProcessingTimePolicy<>();
      }
    };
  }

  /**
   * TODO.
   */
  public static <K, V> TimestampPolicyFactory<K, V> withLogAppendTime() {
    return new TimestampPolicyFactory<K, V>() {
      @Override
      public TimestampPolicy<K, V>
      createTimestampPolicy(TopicPartition tp, Optional<Instant> previousWatermark) {
        return new LogAppendTimePolicy<>(tp, previousWatermark);
      }
    };
  }

  /**
   * Used by the Read transform to support old timestamp functions API.
   */
  static <K, V> TimestampPolicyFactory<K, V> withTimestampFn(
    final SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn) {

    return new TimestampPolicyFactory<K, V>() {
      @Override
      public TimestampPolicy<K, V> createTimestampPolicy(TopicPartition tp,
                                                         Optional<Instant> previousWatermark) {
        return new TimestampFnPolicy<>(timestampFn, previousWatermark);
      }
    };
  }

  /**
   * A simple policy that uses current time for event time and watermark. This should be used
   * only when better timestamps like LogAppendTime are not available for a topic.
   */
  public static class ProcessingTimePolicy<K, V> extends TimestampPolicy<K, V> {

    /**
     * TODO.
     */
    public ProcessingTimePolicy() {
    }

    @Override
    public Instant getTimestampForRecord(KafkaRecord<K, V> record) {
      return Instant.now();
    }

    @Override
    public Instant getWatermark(PartitionContext context) {
      return Instant.now();
    }
  }

  /**
   * TODO.
   */
  public static class LogAppendTimePolicy<K, V> extends TimestampPolicy<K, V> {

    /**
     * When a partition is idle or caught up (i.e. backlog is zero), we advance the watermark
     * to near realtime. Kafka server does not have an API to provide server side current
     * timestamp which could ensure all the future records to have a LogAppendTime later than that.
     * The best we could do is to advance the watermark to
     * 'last backlog check time - small delta to account for any internal buffering in Kafka'.
     * Using 2 seconds for this delta.
     * Should this be user configurable?
     */
    private static final Duration IDLE_WATERMARK_DELTA = Duration.standardSeconds(2);

    protected Instant currentWatermark;

    public LogAppendTimePolicy(TopicPartition unused, Optional<Instant> previousWatermark) {
      currentWatermark = previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    @Override
    public Instant getTimestampForRecord(KafkaRecord<K, V> record) {
      if (record.getTimestampType().equals(KafkaTimestampType.LOG_APPEND_TIME)) {
        currentWatermark = new Instant(record.getTimestamp());
      }
      return currentWatermark;
    }

    @Override
    public Instant getWatermark(PartitionContext context) {
      if (context.backlogInMessages == 0) {
        // The reader is caught up. Update watermark if it is not recent.
        Instant idleWatermark = Instant.now().minus(IDLE_WATERMARK_DELTA);
        if (idleWatermark.isAfter(currentWatermark)) {
          currentWatermark = idleWatermark;
        }
      } // else backlog is > 0 or is unknown, do not update watermark.
      return currentWatermark;
    }
  }

  /**
   * Internal policy to support old deprecated withTimestampFn API. It returns last record
   * timestamp for watermark!.
   */
  private static class TimestampFnPolicy<K, V> extends TimestampPolicy<K, V> {

    final SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn;
    Instant lastRecordTimestamp;

    TimestampFnPolicy(SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn,
                      Optional<Instant> previousWatermark) {
      this.timestampFn = timestampFn;
      lastRecordTimestamp = previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    @Override
    public Instant getTimestampForRecord(KafkaRecord<K, V> record) {
      lastRecordTimestamp = timestampFn.apply(record);
      return lastRecordTimestamp;
    }

    @Override
    public Instant getWatermark(PartitionContext context) {
      return lastRecordTimestamp;
    }
  }
}
