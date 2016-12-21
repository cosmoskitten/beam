package org.apache.beam.runners.spark.coders;

import org.apache.beam.sdk.coders.Coder;

import java.io.Serializable;

/**
 * {@link org.apache.beam.sdk.coders.Coder} wrapper for serialization purposes.
 * See: {@link BeamSparkRunnerRegistrator}.
 */
public class CoderWrapper<T> implements Serializable {
  private final Coder<T> coder;

  public CoderWrapper(Coder<T> coder) {
    this.coder = coder;
  }

  Coder<T> getCoder() {
    return coder;
  }
}
