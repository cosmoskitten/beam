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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
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
 * PCollection<BatchWriteItemRequest> data = ...;
 *
 * data.apply(DynamoDBIO.write()
 *     .withRetryConfiguration(
 *        DynamoDBIO.RetryConfiguration.create(
 *          4, org.joda.time.Duration.standardSeconds(10)))
 *     .withAWSClientsProvider(new BasisDynamoDBProvider(accessKey, secretKey, region))
 *     .withResultOutputTag(results));
 * }</pre>
 *
 * <p>As a client, you need to provide at least the following things:
 *
 * <ul>
 *   <li>retry configuration
 *   <li>need to specify AwsClientsProvider. You can pass on the default one BasisDynamoDBProvider
 *   <li>an output tag where you can get results. Example in DynamoDBIOTest
 * </ul>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public final class DynamoDBIO {
  public static Read read() {
    return new AutoValue_DynamoDBIO_Read.Builder()
        .setNumOfItemPerSegment(Integer.MAX_VALUE)
        .setNumOfSplits(1)
        .build();
  }

  public static Write write() {
    return new AutoValue_DynamoDBIO_Write.Builder().build();
  }

  /** Read data from DynamoDB by BatchGetItemRequest. Todo: doc more */
  @AutoValue
  public abstract static class Read
      extends PTransform<PBegin, PCollection<Map<String, AttributeValue>>> {
    @Nullable
    abstract AwsClientsProvider getAWSClientsProvider();

    @Nullable
    abstract String getTableName();

    @Nullable
    abstract String getFilterExpression();

    @Nullable
    abstract Map<String, AttributeValue> getExpressionAttributeValues();

    @Nullable
    abstract Map<String, String> getExpressionAttributeNames();

    @Nullable
    abstract String getProjectionExpression();

    abstract int getNumOfItemPerSegment();

    abstract int getNumOfSplits();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setAWSClientsProvider(AwsClientsProvider clientProvider);

      abstract Builder setTableName(String tableName);

      abstract Builder setFilterExpression(String filterExpression);

      abstract Builder setExpressionAttributeValues(
          Map<String, AttributeValue> filterExpressionMapValue);

      abstract Builder setExpressionAttributeNames(Map<String, String> filterExpressionMapName);

      abstract Builder setProjectionExpression(String projectionExpression);

      abstract Builder setNumOfItemPerSegment(int numOfItemPerSegment);

      abstract Builder setNumOfSplits(int numOfSplits);

      abstract Read build();
    }
    /**
     * Allows to specify custom {@link AwsClientsProvider}. {@link AwsClientsProvider} provides
     * {@link AmazonDynamoDB} and {@link AmazonCloudWatch} instances which are later used for
     * communication with DynamoDB. You should use this method if {@link
     * Read#withAWSClientsProvider(AwsClientsProvider)} does not suit your needs.
     */
    public Read withAWSClientsProvider(AwsClientsProvider awsClientsProvider) {
      return toBuilder().setAWSClientsProvider(awsClientsProvider).build();
    }

    /**
     * Specify credential details and region to be used to write to dynamo. If you need more
     * sophisticated credential protocol, then you should look at {@link
     * DynamoDBIO.Write#withAWSClientsProvider(AwsClientsProvider)}.
     */
    public Read withAWSClientsProvider(String awsAccessKey, String awsSecretKey, Regions region) {
      return withAWSClientsProvider(awsAccessKey, awsSecretKey, region, null);
    }

    /**
     * Specify credential details and region to be used to write to DynamoDB. If you need more
     * sophisticated credential protocol, then you should look at {@link
     * DynamoDBIO.Write#withAWSClientsProvider(AwsClientsProvider)}.
     *
     * <p>The {@code serviceEndpoint} sets an alternative service host. This is useful to execute
     * the tests with Kinesis service emulator.
     */
    public Read withAWSClientsProvider(
        String awsAccessKey, String awsSecretKey, Regions region, String serviceEndpoint) {
      return withAWSClientsProvider(
          new BasisDynamoDBProvider(awsAccessKey, awsSecretKey, region, serviceEndpoint));
    }

    public Read withTableName(String tableName) {
      return toBuilder().setTableName(tableName).build();
    }

    public Read withFilterExpression(String filterExpression) {
      return toBuilder().setFilterExpression(filterExpression).build();
    }

    public Read withExpressionAttributeNames(Map<String, String> filterExpressionMapName) {
      return toBuilder().setExpressionAttributeNames(filterExpressionMapName).build();
    }

    public Read withExpressionAttributeValues(
        Map<String, AttributeValue> filterExpressionMapValue) {
      return toBuilder().setExpressionAttributeValues(filterExpressionMapValue).build();
    }

    public Read withProjectionExpression(String projectionExpression) {
      return toBuilder().setProjectionExpression(projectionExpression).build();
    }

    public Read withNumOfItemPerSegment(int numOfItemPerSegment) {
      return toBuilder().setNumOfItemPerSegment(numOfItemPerSegment).build();
    }

    public Read withNumOfSplits(int numOfSplits) {
      return toBuilder().setNumOfSplits(numOfSplits).build();
    }

    @Override
    public PCollection<Map<String, AttributeValue>> expand(PBegin input) {

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
     * to resend the requests to the Solr server if the {@link RetryConfiguration} permits it.
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
    abstract AwsClientsProvider getAWSClientsProvider();

    @Nullable
    abstract RetryConfiguration getRetryConfiguration();

    @Nullable
    abstract TupleTag<BatchWriteItemResult> getResultOutputTag();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setAWSClientsProvider(AwsClientsProvider clientProvider);

      abstract Builder setRetryConfiguration(RetryConfiguration retryConfiguration);

      abstract Builder setResultOutputTag(TupleTag<BatchWriteItemResult> results);

      abstract Write build();
    }

    /**
     * Allows to specify custom {@link AwsClientsProvider}. {@link AwsClientsProvider} creates new
     * {@link AmazonDynamoDB} which is later used for writing to a dynamo client database.
     */
    public Write withAWSClientsProvider(AwsClientsProvider awsClientsProvider) {
      return builder().setAWSClientsProvider(awsClientsProvider).build();
    }

    /**
     * Specify credential details and region to be used to write to dynamo. If you need more
     * sophisticated credential protocol, then you should look at {@link
     * DynamoDBIO.Write#withAWSClientsProvider(AwsClientsProvider)}.
     */
    public Write withAWSClientsProvider(String awsAccessKey, String awsSecretKey, Regions region) {
      return withAWSClientsProvider(awsAccessKey, awsSecretKey, region, null);
    }

    /**
     * Specify credential details and region to be used to write to DynamoDB. If you need more
     * sophisticated credential protocol, then you should look at {@link
     * DynamoDBIO.Write#withAWSClientsProvider(AwsClientsProvider)}.
     *
     * <p>The {@code serviceEndpoint} sets an alternative service host. This is useful to execute
     * the tests with Kinesis service emulator.
     */
    public Write withAWSClientsProvider(
        String awsAccessKey, String awsSecretKey, Regions region, String serviceEndpoint) {
      return withAWSClientsProvider(
          new BasisDynamoDBProvider(awsAccessKey, awsSecretKey, region, serviceEndpoint));
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
        client = spec.getAWSClientsProvider().createDynamoDB();
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
