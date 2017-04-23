package org.apache.beam.sdk.metrics;

import com.google.common.base.Joiner;

/**
 * Standard {@link org.apache.beam.sdk.io.Source} Metrics.
 */
public class SourceMetrics {

  private static final String SOURCE_NAMESPACE = "source";
  private static final String SOURCE_SPLITS_NAMESPACE = "source.splits";
  private static final String SEPARATOR = ".";

  private static final String ELEMENTS_READ = "elements_read";
  private static final String BYTES_READ = "bytes_read";
  private static final String BACKLOG_BYTES = "backlog_bytes";
  private static final String BACKLOG_ELEMENTS = "backlog_elements";

  private static final Counter ELEMENTS_READ_COUNTER =
      Metrics.counter(SOURCE_NAMESPACE, ELEMENTS_READ);
  private static final Counter BYTES_READ_COUNTER =
      Metrics.counter(SOURCE_NAMESPACE, BYTES_READ);
  private static final Gauge BACKLOG_BYTES_GAUGE =
      Metrics.gauge(SOURCE_NAMESPACE, BACKLOG_BYTES);
  private static final Gauge BACKLOG_ELEMENTS_GAUGE =
      Metrics.gauge(SOURCE_NAMESPACE, BACKLOG_ELEMENTS);

  /**
   * Counter of elements read by a source.
   */
  public static Counter elementsRead() {
    return ELEMENTS_READ_COUNTER;
  }

  /**
   * Counter of elements read by a source split.
   * Only report a fixed set of Split IDs as not to overload metrics backends.
   */
  public static Counter elementsReadBySplit(String splitId) {
    return Metrics.counter(SOURCE_SPLITS_NAMESPACE, renderName(splitId, ELEMENTS_READ));
  }

  /**
   * Counter of bytes read by a source.
   */
  public static Counter bytesRead() {
    return BYTES_READ_COUNTER;
  }

  /**
   * Counter of bytes read by a source split.
   * Only report a fixed set of Split IDs as not to overload metrics backends.
   */
  public static Counter bytesReadBySplit(String splitId) {
    return Metrics.counter(SOURCE_SPLITS_NAMESPACE, renderName(splitId, BYTES_READ));
  }

  /**
   * Gauge for source backlog in bytes.
   */
  public static Gauge backlogBytes() {
    return BACKLOG_BYTES_GAUGE;
  }

  /**
   * Gauge for source split backlog in bytes.
   * Only report a fixed set of Split IDs as not to overload metrics backends.
   */
  public static Gauge backlogBytesOfSplit(String splitId) {
    return Metrics.gauge(SOURCE_SPLITS_NAMESPACE, renderName(splitId, BACKLOG_BYTES));
  }

  /**
   * Gauge for source backlog in elements.
   */
  public static Gauge backlogElements() {
    return BACKLOG_ELEMENTS_GAUGE;
  }

  /**
   * Gauge for source split backlog in elements.
   * Only report a fixed set of Split IDs as not to overload metrics backends.
   */
  public static Gauge backlogElementsOfSplit(String splitId) {
    return Metrics.gauge(SOURCE_SPLITS_NAMESPACE, renderName(splitId, BACKLOG_ELEMENTS));
  }

  private static String renderName(String... nameParts) {
    return Joiner.on(SEPARATOR).join(nameParts);
  }
}
