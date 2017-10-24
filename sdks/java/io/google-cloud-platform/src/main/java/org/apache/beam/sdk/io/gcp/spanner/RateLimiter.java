package org.apache.beam.sdk.io.gcp.spanner;

import java.io.Serializable;

public interface RateLimiter {

  void start();

  void end();

  interface Factory extends Serializable {

    RateLimiter create();
  }

}
