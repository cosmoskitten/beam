package org.apache.beam.sdk.io.kafka;

/**
 * This is a copy of Kafka's {@link org.apache.kafka.common.record.TimestampType}. Included
 * here in order to support older Kafka versions (0.9.x).
 */
public enum KafkaTimestampType {
  NO_TIMESTAMP_TYPE(-1, "NoTimestampType"),
  CREATE_TIME(0, "CreateTime"),
  LOG_APPEND_TIME(1, "LogAppendTime");

  public final int id;
  public final String name;

  KafkaTimestampType(int id, String name) {
    this.id = id;
    this.name = name;
  }

  public static KafkaTimestampType forOrdinal(int ordinal) {
    return values()[ordinal];
  }

  @Override
  public String toString() {
    return name;
  }
}
