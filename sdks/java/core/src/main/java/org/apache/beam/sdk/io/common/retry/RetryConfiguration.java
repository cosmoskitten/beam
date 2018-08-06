package org.apache.beam.sdk.io.common.retry;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import org.joda.time.Duration;

/**
 * A POJO encapsulating a configuration for retry behavior when issuing requests. A retry will be
 * attempted until the maxAttempts or maxDuration is exceeded, whichever comes first, for any of the
 * following exceptions:
 */
public class RetryConfiguration implements Serializable {

  private int maxAttempts;
  private Duration maxDuration;
  private RetryPredicate retryPredicate;

  public int getMaxAttempts() {
    return maxAttempts;
  }

  public Duration getMaxDuration() {
    return maxDuration;
  }

  public RetryPredicate getRetryPredicate() {
    return retryPredicate;
  }

  public static RetryConfiguration create(
      int maxAttempts, Duration maxDuration, RetryPredicate predicate) {
    checkArgument(maxAttempts > 0, "maxAttempts must be greater than 0");
    checkArgument(
        maxDuration != null && maxDuration.isLongerThan(Duration.ZERO),
        "maxDuration must be greater than 0");
    return RetryConfigurationBuilder.create()
        .withMaxAttempts(maxAttempts)
        .withMaxDuration(maxDuration)
        .withRetryPredicate(predicate)
        .build();
  }

  public static final class RetryConfigurationBuilder {

    private int maxAttempts;
    private Duration maxDuration;
    private RetryPredicate retryPredicate;

    public static RetryConfigurationBuilder create() {
      return new RetryConfigurationBuilder();
    }

    private RetryConfigurationBuilder() {}

    public RetryConfigurationBuilder withMaxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public RetryConfigurationBuilder withMaxDuration(Duration maxDuration) {
      this.maxDuration = maxDuration;
      return this;
    }

    public RetryConfigurationBuilder withRetryPredicate(RetryPredicate retryPredicate) {
      this.retryPredicate = retryPredicate;
      return this;
    }

    public RetryConfiguration build() {
      RetryConfiguration retryConfiguration = new RetryConfiguration();
      retryConfiguration.retryPredicate = this.retryPredicate;
      retryConfiguration.maxDuration = this.maxDuration;
      retryConfiguration.maxAttempts = this.maxAttempts;
      return retryConfiguration;
    }
  }
}
