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
package org.apache.beam.sdk.io.aws.dynamodb;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableSet;
import org.apache.http.HttpStatus;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform}s for writing to <a href="https://aws.amazon.com/dynamodb/">DynamoDB</a>.
 *
 * <h3>Writing to DynamoDB</h3>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * DynamoDBIO.DynamoDBConfiguration config = DynamoDBIO.DynamoDBConfiguration.create(
 *     "region", "accessKey", "secretKey");
 * PCollection<BatchWriteItemRequest> data = ...;
 *
 * data.apply(DynamoDBIO.write()
 *     .withRetryConfiguration(
 *        DynamoDBIO.RetryConfiguration.create(
 *          4, org.joda.time.Duration.standardSeconds(10)))
 *     .withDynamoDBConfiguration(config)
 *     .withResultOutputTag(results));
 * }</pre>
 *
 * <p>As a client, you need to provide at least the following things:
 *
 * <ul>
 *   <li>Retry configuration
 *   <li>DynamoDb configuration
 *   <li>An output tag where you can get results. Example in DynamoDBIOTest
 * </ul>
 *
 * <h3>Reading from DynamoDB</h3>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * DynamoDBIO.DynamoDBConfiguration config = DynamoDBIO.DynamoDBConfiguration.create(
 *     "endpointUrl", "region", "accessKey", "secretKey");
 * PCollection<Map<String, AttributeValue>> actual =
 *     pipeline.apply(DynamoDBIO.read()
 *         .withScanRequestFn((v) -> new ScanRequest(tableName).withTotalSegment(10))
 *         .withDynamoDBConfiguration(config));
 * }</pre>
 *
 * <p>As a client, you need to provide at least the following things:
 *
 * <ul>
 *   <li>DynamoDb configuration
 *   <li>ScanRequestFn, which you build a ScanRequest object with at least table name and total
 *       number of segment. Note This number should base on the number of your workers
 * </ul>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public final class DynamoDBIO {
  public static Read read() {
    return new AutoValue_DynamoDBIO_Read.Builder().build();
  }

  public static Write write() {
    return new AutoValue_DynamoDBIO_Write.Builder().build();
  }

  /** A config object used to construct AmazonDynamoDB client object. */
  @AutoValue
  public abstract static class DynamoDBConfiguration implements Serializable {
    @Nullable
    abstract ValueProvider<String> getRegion();

    @Nullable
    abstract ValueProvider<String> getEndpointUrl();

    @Nullable
    abstract ValueProvider<String> getAwsAccessKey();

    @Nullable
    abstract ValueProvider<String> getAwsSecretKey();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setRegion(ValueProvider<String> region);

      abstract Builder setEndpointUrl(ValueProvider<String> endpointUrl);

      abstract Builder setAwsAccessKey(ValueProvider<String> accessKey);

      abstract Builder setAwsSecretKey(ValueProvider<String> secretKey);

      abstract DynamoDBConfiguration build();
    }

    public static DynamoDBConfiguration create(String region, String accessKey, String secretKey) {
      checkArgument(region != null, "region can not be null");
      checkArgument(accessKey != null, "accessKey can not be null");
      checkArgument(secretKey != null, "secretKey can not be null");
      return create(
          null,
          ValueProvider.StaticValueProvider.of(region),
          ValueProvider.StaticValueProvider.of(accessKey),
          ValueProvider.StaticValueProvider.of(secretKey));
    }

    public static DynamoDBConfiguration create(
        String endpointUrl, String region, String accessKey, String secretKey) {
      checkArgument(region != null, "region can not be null");
      checkArgument(accessKey != null, "accessKey can not be null");
      checkArgument(secretKey != null, "secretKey can not be null");
      return create(
          ValueProvider.StaticValueProvider.of(endpointUrl),
          ValueProvider.StaticValueProvider.of(region),
          ValueProvider.StaticValueProvider.of(accessKey),
          ValueProvider.StaticValueProvider.of(secretKey));
    }

    public static DynamoDBConfiguration create(
        ValueProvider<String> endpointUrl,
        ValueProvider<String> region,
        ValueProvider<String> accessKey,
        ValueProvider<String> secretKey) {
      checkArgument(region != null, "region can not be null");
      checkArgument(accessKey != null, "accessKey can not be null");
      checkArgument(secretKey != null, "secretKey can not be null");
      return new AutoValue_DynamoDBIO_DynamoDBConfiguration.Builder()
          .setEndpointUrl(endpointUrl)
          .setRegion(region)
          .setAwsAccessKey(accessKey)
          .setAwsSecretKey(secretKey)
          .build();
    }

    public DynamoDBConfiguration withRegion(String region) {
      return withRegion(ValueProvider.StaticValueProvider.of(region));
    }

    public DynamoDBConfiguration withRegion(ValueProvider<String> region) {
      return builder().setRegion(region).build();
    }

    public DynamoDBConfiguration withEndpointUrl(String endpointUrl) {
      return withEndpointUrl(ValueProvider.StaticValueProvider.of(endpointUrl));
    }

    public DynamoDBConfiguration withEndpointUrl(ValueProvider<String> endpointUrl) {
      return builder().setRegion(endpointUrl).build();
    }

    public DynamoDBConfiguration withAwsAccessKey(String accessKey) {
      return withAwsAccessKey(ValueProvider.StaticValueProvider.of(accessKey));
    }

    public DynamoDBConfiguration withAwsAccessKey(ValueProvider<String> accessKey) {
      return builder().setAwsAccessKey(accessKey).build();
    }

    public DynamoDBConfiguration withAwsSecretKey(String secretKey) {
      return withAwsSecretKey(ValueProvider.StaticValueProvider.of(secretKey));
    }

    public DynamoDBConfiguration withAwsSecretKey(ValueProvider<String> secretKey) {
      return builder().setAwsSecretKey(secretKey).build();
    }

    AmazonDynamoDB buildAmazonDynamoDB() {
      AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard();
      if (getEndpointUrl() != null) {
        builder.setEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(getEndpointUrl().get(), getRegion().get()));
      }
      if (getEndpointUrl() == null && getRegion() != null) {
        builder.setRegion(getRegion().get());
      }
      if (getAwsAccessKey() != null && getAwsSecretKey() != null) {
        builder.setCredentials(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(getAwsAccessKey().get(), getAwsSecretKey().get())));
      }
      return builder.build();
    }
  }

  /** Read data from DynamoDB and return PCollection<Map<String, AttributeValue>>. */
  @AutoValue
  public abstract static class Read
      extends PTransform<PBegin, PCollection<Map<String, AttributeValue>>> {
    @Nullable
    abstract DynamoDBConfiguration getDynamoDBConfiguration();

    @Nullable
    abstract SerializableFunction<Void, ScanRequest> getScanRequestFn();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setDynamoDBConfiguration(DynamoDBConfiguration dynamoDBConfiguration);

      abstract Builder setScanRequestFn(SerializableFunction<Void, ScanRequest> fn);

      abstract Read build();
    }

    public Read withDynamoDBConfiguration(DynamoDBConfiguration dynamoDBConfiguration) {
      return toBuilder().setDynamoDBConfiguration(dynamoDBConfiguration).build();
    }

    public Read withScanRequestFn(SerializableFunction<Void, ScanRequest> fn) {
      return toBuilder().setScanRequestFn(fn).build();
    }

    @Override
    public PCollection<Map<String, AttributeValue>> expand(PBegin input) {
      checkArgument((getScanRequestFn() != null), "withScanRequestFn() is required");
      checkArgument(
          (getDynamoDBConfiguration() != null), "withDynamoDBConfiguration() is required");
      checkArgument(
          (getScanRequestFn().apply(null).getTotalSegments() != null
              && getScanRequestFn().apply(null).getTotalSegments() > 0),
          "TotalSegments is required with withScanRequestFn() and greater zero");

      return input.apply(org.apache.beam.sdk.io.Read.from(new DynamoDBBoundedSource(this, 0)));
    }
  }

  /**
   * A POJO encapsulating a configuration for retry behavior when issuing requests to dynamodb. A
   * retry will be attempted until the maxAttempts or maxDuration is exceeded, whichever comes
   * first, for any of the following exceptions:
   *
   * <ul>
   *   <li>{@link IOException}
   * </ul>
   */
  @AutoValue
  public abstract static class RetryConfiguration implements Serializable {
    @VisibleForTesting
    static final RetryPredicate DEFAULT_RETRY_PREDICATE = new DefaultRetryPredicate();

    abstract int getMaxAttempts();

    abstract Duration getMaxDuration();

    abstract DynamoDBIO.RetryConfiguration.RetryPredicate getRetryPredicate();

    abstract DynamoDBIO.RetryConfiguration.Builder builder();

    public static DynamoDBIO.RetryConfiguration create(int maxAttempts, Duration maxDuration) {
      checkArgument(maxAttempts > 0, "maxAttempts should be greater than 0");
      checkArgument(
          maxDuration != null && maxDuration.isLongerThan(Duration.ZERO),
          "maxDuration should be greater than 0");
      return new AutoValue_DynamoDBIO_RetryConfiguration.Builder()
          .setMaxAttempts(maxAttempts)
          .setMaxDuration(maxDuration)
          .setRetryPredicate(DEFAULT_RETRY_PREDICATE)
          .build();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract DynamoDBIO.RetryConfiguration.Builder setMaxAttempts(int maxAttempts);

      abstract DynamoDBIO.RetryConfiguration.Builder setMaxDuration(Duration maxDuration);

      abstract DynamoDBIO.RetryConfiguration.Builder setRetryPredicate(
          RetryPredicate retryPredicate);

      abstract DynamoDBIO.RetryConfiguration build();
    }

    /**
     * An interface used to control if we retry the BatchWriteItemRequest call when a {@link
     * Throwable} occurs. If {@link RetryPredicate#test(Object)} returns true, {@link Write} tries
     * to resend the requests to the dynamodb server if the {@link RetryConfiguration} permits it.
     */
    @FunctionalInterface
    interface RetryPredicate extends Predicate<Throwable>, Serializable {}

    private static class DefaultRetryPredicate implements RetryPredicate {
      private static final ImmutableSet<Integer> ELIGIBLE_CODES =
          ImmutableSet.of(HttpStatus.SC_SERVICE_UNAVAILABLE);

      @Override
      public boolean test(Throwable throwable) {
        return (throwable instanceof IOException
            || (throwable instanceof AmazonDynamoDBException)
            || (throwable instanceof AmazonDynamoDBException
                && ELIGIBLE_CODES.contains(((AmazonDynamoDBException) throwable).getStatusCode())));
      }
    }
  }

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write
      extends PTransform<PCollection<BatchWriteItemRequest>, PCollectionTuple> {

    @Nullable
    abstract DynamoDBConfiguration getDynamoDBConfiguration();

    @Nullable
    abstract RetryConfiguration getRetryConfiguration();

    @Nullable
    abstract TupleTag<BatchWriteItemResult> getResultOutputTag();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setDynamoDBConfiguration(DynamoDBConfiguration dynamoDBConfiguration);

      abstract Builder setRetryConfiguration(RetryConfiguration retryConfiguration);

      abstract Builder setResultOutputTag(TupleTag<BatchWriteItemResult> results);

      abstract Write build();
    }

    public Write withDynamoDBConfiguration(DynamoDBConfiguration dynamoDBConfiguration) {
      return builder().setDynamoDBConfiguration(dynamoDBConfiguration).build();
    }

    /**
     * Provides configuration to retry a failed request to publish a set of records to DynamoDB.
     * Users should consider that retrying might compound the underlying problem which caused the
     * initial failure. Users should also be aware that once retrying is exhausted the error is
     * surfaced to the runner which <em>may</em> then opt to retry the current partition in entirety
     * or abort if the max number of retries of the runner is completed. Retrying uses an
     * exponential backoff algorithm, with minimum backoff of 5 seconds and then surfacing the error
     * once the maximum number of retries or maximum configuration duration is exceeded.
     *
     * <p>Example use:
     *
     * <pre>{@code
     * DynamoDBIO.write()
     *   .withRetryConfiguration(DynamoDBIO.RetryConfiguration.create(5, Duration.standardMinutes(1))
     *   ...
     * }</pre>
     *
     * @param retryConfiguration the rules which govern the retry behavior
     * @return the {@link DynamoDBIO.Write} with retrying configured
     */
    public Write withRetryConfiguration(RetryConfiguration retryConfiguration) {
      checkArgument(retryConfiguration != null, "retryConfiguration is required");
      return builder().setRetryConfiguration(retryConfiguration).build();
    }

    /** Tuple tag to store results. Mandatory field. */
    public Write withResultOutputTag(TupleTag<BatchWriteItemResult> results) {
      return builder().setResultOutputTag(results).build();
    }

    @Override
    public PCollectionTuple expand(PCollection<BatchWriteItemRequest> input) {
      return input.apply(
          ParDo.of(new DynamoDbWriterFn(this))
              .withOutputTags(getResultOutputTag(), TupleTagList.empty()));
    }

    static class DynamoDbWriterFn extends DoFn<BatchWriteItemRequest, BatchWriteItemResult> {
      @VisibleForTesting
      static final String RETRY_ATTEMPT_LOG = "Error writing to DynamoDB. Retry attempt[%d]";

      private static final Duration RETRY_INITIAL_BACKOFF = Duration.standardSeconds(5);
      private transient FluentBackoff retryBackoff; // defaults to no retries
      private static final Logger LOG =
          LoggerFactory.getLogger(DynamoDBIO.Write.DynamoDbWriterFn.class);
      private static final Counter DYNAMO_DB_WRITE_FAILURES =
          Metrics.counter(DynamoDBIO.Write.DynamoDbWriterFn.class, "DynamoDB_Write_Failures");

      private transient AmazonDynamoDB client;
      private final DynamoDBIO.Write spec;

      DynamoDbWriterFn(DynamoDBIO.Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        client = spec.getDynamoDBConfiguration().buildAmazonDynamoDB();
        retryBackoff =
            FluentBackoff.DEFAULT
                .withMaxRetries(0) // default to no retrying
                .withInitialBackoff(RETRY_INITIAL_BACKOFF);
        if (spec.getRetryConfiguration() != null) {
          retryBackoff =
              retryBackoff
                  .withMaxRetries(spec.getRetryConfiguration().getMaxAttempts() - 1)
                  .withMaxCumulativeBackoff(spec.getRetryConfiguration().getMaxDuration());
        }
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        final BatchWriteItemRequest writeRequest = context.element();
        Sleeper sleeper = Sleeper.DEFAULT;
        BackOff backoff = retryBackoff.backoff();
        int attempt = 0;
        while (true) {
          attempt++;
          try {
            final BatchWriteItemResult batchWriteItemResult = client.batchWriteItem(writeRequest);
            context.output(batchWriteItemResult);
            break;
          } catch (Exception ex) {
            // Fail right away if there is no retry configuration
            if (spec.getRetryConfiguration() == null
                || !spec.getRetryConfiguration().getRetryPredicate().test(ex)) {
              DYNAMO_DB_WRITE_FAILURES.inc();
              LOG.info(
                  "Unable to write batch items {} due to {} ",
                  writeRequest.getRequestItems().entrySet(),
                  ex);
              throw new IOException("Error writing to DyanmoDB (no attempt made to retry)", ex);
            }

            if (!BackOffUtils.next(sleeper, backoff)) {
              throw new IOException(
                  String.format(
                      "Error writing to DyanmoDB after %d attempt(s). No more attempts allowed",
                      attempt),
                  ex);
            } else {
              // Note: this used in test cases to verify behavior
              LOG.warn(String.format(RETRY_ATTEMPT_LOG, attempt), ex);
            }
          }
        }
      }

      @Teardown
      public void tearDown() {
        if (client != null) {
          client.shutdown();
          client = null;
        }
      }
    }
  }
}
