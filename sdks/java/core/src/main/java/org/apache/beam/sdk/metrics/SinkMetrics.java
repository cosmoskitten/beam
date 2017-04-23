package org.apache.beam.sdk.metrics;

/**
 * Standard Sink Metrics.
 */
public class SinkMetrics {

  private static final String SINK_NAMESPACE = "sink";

  private static final String ELEMENTS_WRITTEN = "elements_written";
  private static final String BYTES_WRITTEN = "bytes_written";

  private static final Counter ELEMENTS_WRITTEN_COUNTER =
      Metrics.counter(SINK_NAMESPACE, ELEMENTS_WRITTEN);
  private static final Counter BYTES_WRITTEN_COUNTER =
      Metrics.counter(SINK_NAMESPACE, BYTES_WRITTEN);

  /**
   * Counter of elements written to a sink.
   */
  public static Counter elementsWritten() {
    return ELEMENTS_WRITTEN_COUNTER;
  }

  /**
   * Counter of bytes written to a sink.
   */
  public static Counter bytesWritten() {
    return BYTES_WRITTEN_COUNTER;
  }

}
