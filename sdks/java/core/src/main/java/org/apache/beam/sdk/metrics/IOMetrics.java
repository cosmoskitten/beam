/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.metrics;

import com.google.common.base.Joiner;

/**
 * Standard IO Metrics.
 */
public class IOMetrics {

  private static final String IO_NAMESPACE = "io";
  private static final String IO_SPLITS_NAMESPACE = "io.splits";
  private static final String SEPARATOR = ".";

  private static final String ELEMENTS_READ = "elements_read";
  private static final String BYTES_READ = "bytes_read";
  private static final String BACKLOG_BYTES = "backlog_bytes";
  private static final String BACKLOG_ELEMENTS = "backlog_elements";
  private static final String ELEMENTS_WRITTEN = "elements_written";
  private static final String BYTES_WRITTEN = "bytes_written";

  private static final Counter ELEMENTS_READ_COUNTER = Metrics.counter(IO_NAMESPACE, ELEMENTS_READ);
  private static final Counter BYTES_READ_COUNTER = Metrics.counter(IO_NAMESPACE, BYTES_READ);
  private static final Counter ELEMENTS_WRITTEN_COUNTER =
      Metrics.counter(IO_NAMESPACE, ELEMENTS_WRITTEN);
  private static final Counter BYTES_WRITTEN_COUNTER =
      Metrics.counter(IO_NAMESPACE, BYTES_WRITTEN);
  private static final Gauge BACKLOG_BYTES_GAUGE = Metrics.gauge(IO_NAMESPACE, BACKLOG_BYTES);
  private static final Gauge BACKLOG_ELEMENTS_GAUGE = Metrics.gauge(IO_NAMESPACE, BACKLOG_ELEMENTS);

  // ------------------------- Source Metrics -------------------------

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
    return Metrics.counter(IO_SPLITS_NAMESPACE, renderName(splitId, ELEMENTS_READ));
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
    return Metrics.counter(IO_SPLITS_NAMESPACE, renderName(splitId, BYTES_READ));
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
    return Metrics.gauge(IO_SPLITS_NAMESPACE, renderName(splitId, BACKLOG_BYTES));
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
    return Metrics.gauge(IO_SPLITS_NAMESPACE, renderName(splitId, BACKLOG_ELEMENTS));
  }

  // ------------------------- Sink Metrics -------------------------

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

  // ----------------------------------------------------------------

  private static String renderName(String... nameParts) {
    return Joiner.on(SEPARATOR).join(nameParts);
  }
}
