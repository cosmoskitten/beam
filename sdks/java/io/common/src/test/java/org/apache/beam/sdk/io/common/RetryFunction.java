package org.apache.beam.sdk.io.common;

/**
 * Interface for passing method to retry static method in {@link
 * org.apache.beam.sdk.io.common.IOITHelper}.
 */
public interface RetryFunction {
  void run() throws Exception;
}
