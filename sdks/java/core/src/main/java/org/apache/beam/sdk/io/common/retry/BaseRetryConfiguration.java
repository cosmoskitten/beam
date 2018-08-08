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
package org.apache.beam.sdk.io.common.retry;

import java.io.Serializable;
import org.joda.time.Duration;

/**
 * A POJO encapsulating a configuration for retry behavior when issuing requests. A retry will be
 * attempted until the maxAttempts or maxDuration is exceeded, whichever comes first.
 */
public class BaseRetryConfiguration implements Serializable {

  private int maxAttempts;
  private Duration maxDuration;
  protected RetryPredicate retryPredicate;

  protected BaseRetryConfiguration(
      int maxAttempts, Duration maxDuration, RetryPredicate retryPredicate) {
    super();
    this.maxAttempts = maxAttempts;
    this.maxDuration = maxDuration;
    this.retryPredicate = retryPredicate;
  }

  public int getMaxAttempts() {
    return maxAttempts;
  }

  public Duration getMaxDuration() {
    return maxDuration;
  }

  public RetryPredicate getRetryPredicate() {
    return retryPredicate;
  }
}
