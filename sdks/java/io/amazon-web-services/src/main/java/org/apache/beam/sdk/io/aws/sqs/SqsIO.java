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
package org.apache.beam.sdk.io.aws.sqs;

import static com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.io.aws.sqs.SqsIO.Write.Builder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;

/** An unbounded source for Amazon Simple Queue Service (SQS). */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SqsIO {

  public static Read read() {
    return new AutoValue_SqsIO_Read.Builder().setMaxNumRecords(Long.MAX_VALUE).build();
  }

  public static Write write() {
    return new AutoValue_SqsIO_Write.Builder().build();
  }

  private SqsIO() {}

  /** A {@link PTransform} to read/receive messages from SQS. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Message>> {

    @Nullable
    abstract String queueUrl();

    abstract long maxNumRecords();

    @Nullable
    abstract Duration maxReadTime();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setQueueUrl(String queueUrl);

      abstract Builder setMaxNumRecords(long maxNumRecords);

      abstract Builder setMaxReadTime(Duration maxReadTime);

      abstract Read build();
    }

    /**
     * Define the max number of records received by the {@link Read}. When the max number of records
     * is lower than {@code Long.MAX_VALUE}, the {@link Read} will provide a bounded {@link
     * PCollection}.
     */
    public Read withMaxNumRecords(long maxNumRecords) {
      return toBuilder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time (duration) while the {@link Read} will receive messages. When this
     * max read time is not null, the {@link Read} will provide a bounded {@link PCollection}.
     */
    public Read withMaxReadTime(Duration maxReadTime) {
      return toBuilder().setMaxReadTime(maxReadTime).build();
    }

    /** Define the queueUrl used by the {@link Read} to receive messages from SQS. */
    public Read withQueueUrl(String queueUrl) {
      checkArgument(queueUrl != null, "queueUrl can not be null");
      checkArgument(!queueUrl.isEmpty(), "queueUrl can not be empty");
      return toBuilder().setQueueUrl(queueUrl).build();
    }

    @Override
    public PCollection<Message> expand(PBegin input) {

      org.apache.beam.sdk.io.Read.Unbounded<Message> unbounded =
          org.apache.beam.sdk.io.Read.from(
              new SqsUnboundedSource(
                  this,
                  new SqsConfiguration(input.getPipeline().getOptions().as(AwsOptions.class))));

      PTransform<PBegin, PCollection<Message>> transform = unbounded;

      if (maxNumRecords() < Long.MAX_VALUE || maxReadTime() != null) {
        transform = unbounded.withMaxReadTime(maxReadTime()).withMaxNumRecords(maxNumRecords());
      }

      return input.getPipeline().apply(transform);
    }
  }

  /** A {@link PTransform} to send messages to SQS. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, PDone> {
    @Nullable
    abstract String queueUrl();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setQueueUrl(String queueUrl);

      abstract Write build();
    }

    /** Define the queueUrl used by the {@link Write} to send messages to SQS. */
    public Write withQueueUrl(String queueUrl) {
      checkArgument(queueUrl != null, "queueUrl can not be null");
      checkArgument(!queueUrl.isEmpty(), "queueUrl can not be empty");
      return toBuilder().setQueueUrl(queueUrl).build();
    }

    @Override
    public PDone expand(PCollection<String> input) {
      input.apply(
          ParDo.of(
              new SqsWriteFn(
                  new SqsConfiguration(input.getPipeline().getOptions().as(AwsOptions.class)),
                  queueUrl())));
      return PDone.in(input.getPipeline());
    }
  }

  private static class SqsWriteFn extends DoFn<String, Void> {
    private final SqsConfiguration sqsConfiguration;
    private final String queueUrl;
    private transient AmazonSQS sqs;

    SqsWriteFn(SqsConfiguration sqsConfiguration, String queueUrl) {
      this.sqsConfiguration = sqsConfiguration;
      this.queueUrl = queueUrl;
    }

    @Setup
    public void setup() {
      sqs =
          AmazonSQSClientBuilder.standard()
              .withClientConfiguration(sqsConfiguration.getClientConfiguration())
              .withCredentials(sqsConfiguration.getAwsCredentialsProvider())
              .withRegion(sqsConfiguration.getAwsRegion())
              .build();
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {
      sqs.sendMessage(queueUrl, processContext.element());
    }

    @Teardown
    public void teardown() throws Exception {
      if (sqs != null) {
        sqs.shutdown();
      }
    }
  }
}
