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
package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.util.BackOff;
import org.joda.time.Duration;


/**
 * A {@link BackOff} for which the retry algorithm is customizable.
 */
public final class FlexibleBackoff implements BackOff {
  private static final double DEFAULT_EXPONENT = 1.5;
  private static final double DEFAULT_RANDOMIZATION_FACTOR = 0.5;
  private static final Duration DEFAULT_MIN_BACKOFF = Duration.standardSeconds(1);
  private static final Duration DEFAULT_MAX_BACKOFF = Duration.standardDays(1000);
  private static final int DEFAULT_MAX_ATTEMPTS = Integer.MAX_VALUE;
  private static final Duration DEFAULT_MAX_CUM_BACKOFF = Duration.standardDays(1000);
  // Customization of this backoff.
  private final double exponent;
  private final Duration initialBackoff;
  private final Duration maxBackoff;
  private final Duration maxCumulativeBackoff;
  private final int maxAttempts;
  // Current state
  private Duration currentCumulativeBackoff;
  private int currentAttempt;

  public static FlexibleBackoff of() {
    return new FlexibleBackoff(
        DEFAULT_EXPONENT,
        DEFAULT_MIN_BACKOFF, DEFAULT_MAX_BACKOFF, DEFAULT_MAX_CUM_BACKOFF,
        DEFAULT_MAX_ATTEMPTS);
  }

  public FlexibleBackoff withExponent(double exponent) {
    checkArgument(exponent > 0, "exponent %s must be greater than 0", exponent);
    return new FlexibleBackoff(
        exponent, initialBackoff, maxBackoff, maxCumulativeBackoff, maxAttempts);
  }

  public FlexibleBackoff withInitialBackoff(Duration initialBackoff) {
    checkArgument(
        initialBackoff.isLongerThan(Duration.ZERO),
        "initialBackoff %s must be at least 1 millisecond",
        initialBackoff);
    return new FlexibleBackoff(
        exponent, initialBackoff, maxBackoff, maxCumulativeBackoff, maxAttempts);
  }

  public FlexibleBackoff withMaxBackoff(Duration maxBackoff) {
    checkArgument(
        maxBackoff.getMillis() > 0,
        "maxBackoff %s must be at least 1 millisecond",
        maxBackoff);
    return new FlexibleBackoff(
        exponent, initialBackoff, maxBackoff, maxCumulativeBackoff, maxAttempts);
  }

  public FlexibleBackoff withMaxCumulativeBackoff(Duration maxCumulativeBackoff) {
    checkArgument(maxCumulativeBackoff.isLongerThan(Duration.ZERO),
        "maxCumulativeBackoff %s must be at least 1 millisecond", maxCumulativeBackoff);
    return new FlexibleBackoff(
        exponent, initialBackoff, maxBackoff, maxCumulativeBackoff, maxAttempts);
  }

  public FlexibleBackoff withMaxAttempts(int maxAttempts) {
    checkArgument(maxAttempts >= 1, "maxAttempts %s must be at least 1", maxAttempts);
    return new FlexibleBackoff(
        exponent, initialBackoff, maxBackoff, maxCumulativeBackoff, maxAttempts);
  }

  @Override
  public void reset() {
    currentAttempt = 0;
    currentCumulativeBackoff = Duration.ZERO;
  }

  @Override
  public long nextBackOffMillis() {
    // Maximum number of attempts reached.
    if (currentAttempt >= maxAttempts) {
      return BackOff.STOP;
    }
    // Maximum cumulative backoff reached.
    if (currentCumulativeBackoff.compareTo(maxCumulativeBackoff) >= 0) {
      return BackOff.STOP;
    }

    double currentIntervalMillis =
        Math.min(
            initialBackoff.getMillis() * Math.pow(exponent, currentAttempt),
            maxBackoff.getMillis());
    double randomOffset =
        (Math.random() * 2 - 1) * DEFAULT_RANDOMIZATION_FACTOR * currentIntervalMillis;
    long nextBackoffMillis = Math.round(currentIntervalMillis + randomOffset);
    // Cap to limit on cumulative backoff
    Duration remainingCumulative = maxCumulativeBackoff.minus(currentCumulativeBackoff);
    nextBackoffMillis = Math.min(nextBackoffMillis, remainingCumulative.getMillis());

    // Update state and return backoff.
    currentCumulativeBackoff = currentCumulativeBackoff.plus(nextBackoffMillis);
    currentAttempt += 1;
    return nextBackoffMillis;
  }

  public FlexibleBackoff copy() {
    return new FlexibleBackoff(
        exponent, initialBackoff, maxBackoff, maxCumulativeBackoff, maxAttempts);
  }

  private FlexibleBackoff(
      double exponent, Duration initialBackoff, Duration maxBackoff, Duration maxCumulativeBackoff,
      int maxAttempts) {
    this.exponent = exponent;
    this.initialBackoff = initialBackoff;
    this.maxBackoff = maxBackoff;
    this.maxAttempts = maxAttempts;
    this.maxCumulativeBackoff = maxCumulativeBackoff;
    reset();
  }
}
