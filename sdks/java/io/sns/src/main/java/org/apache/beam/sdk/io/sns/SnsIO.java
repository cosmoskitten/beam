package org.apache.beam.sdk.io.sns;

import static com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.GetTopicAttributesResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform}s for writing to <a href="https://aws.amazon.com/sns/">SNS</a>.
 *
 * <h3>Writing to SNS</h3>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PCollection<PublishRequest> data = ...;
 *
 * data.apply(SnsIO.write()
 *     .withTopicName("topicName")
 *     .withMaxRetries(MAX_RETRIES) //eg. 2
 *     .withMaxDelay(MAX_DELAY) //eg. Duration.ofSeconds(2)
 *     .withRetryDelay(RETRY_DELAY) //eg. Duration.ofSeconds(1)
 *     .withAwsClientsProvider(new Provider(new AmazonSNSMock()))
 *     .withResultOutputTag(results));
 * }</pre>
 *
 * <p>As a client, you need to provide at least the following things:
 *
 * <ul>
 *   <li>name of the SNS topic you're going to write to
 *   <li>the max number of retries to perform.
 *   <li>maximum total delay
 *   <li>delay between retries
 *   <li>need to specify AwsClientsProvider. You can pass on the default one
 *       DefaultSnsClientProvider
 *   <li>an output tag where you can get results. Example in SnsIOTest
 * </ul>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public final class SnsIO {

  private static final int DEFAULT_MAX_RETRIES = 6;

  //Write data tp SNS
  public static Write write() {
    return new AutoValue_SnsIO_Write.Builder().setMaxRetries(DEFAULT_MAX_RETRIES).build();
  }

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write
      extends PTransform<PCollection<PublishRequest>, PCollectionTuple> {
    @Nullable
    abstract String getTopicName();

    @Nullable
    abstract AwsClientsProvider getAwsClientsProvider();

    @Nullable
    abstract Duration getMaxDelay();

    @Nonnull
    abstract int getMaxRetries();

    @Nullable
    abstract Duration getRetryDelay();

    @Nullable
    abstract TupleTag<PublishResult> getResultOutputTag();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setTopicName(String topicName);

      abstract Builder setAwsClientsProvider(AwsClientsProvider clientProvider);

      abstract Builder setMaxDelay(Duration maxDelay);

      abstract Builder setMaxRetries(int maxRetries);

      abstract Builder setRetryDelay(Duration retryDelay);

      abstract Builder setResultOutputTag(TupleTag<PublishResult> results);

      abstract Write build();
    }

    /** Specify the SNS topic which will be used for writing, this name is mandatory. */
    public Write withTopicName(String topicName) {
      return builder().setTopicName(topicName).build();
    }

    /**
     * Allows to specify custom {@link AwsClientsProvider}. {@link AwsClientsProvider} creates new
     * {@link AmazonSNS} which is later used for writing to a SNS topic.
     */
    public Write withAwsClientsProvider(AwsClientsProvider awsClientsProvider) {
      return builder().setAwsClientsProvider(awsClientsProvider).build();
    }

    /** Max delay in seconds for retries. Mandatory field. */
    public Write withMaxDelay(Duration maxDelay) {
      return builder().setMaxDelay(maxDelay).build();
    }

    /** Max number of times to retry before writing to failures. Mandatory field. */
    public Write withMaxRetries(int maxRetries) {
      return builder().setMaxRetries(maxRetries).build();
    }

    /** Initial delay in seconds for retries. Mandatory field. */
    public Write withRetryDelay(Duration retryDelay) {
      return builder().setRetryDelay(retryDelay).build();
    }

    /** Tuple tag to store results. Mandatory field. */
    public Write withResultOutputTag(TupleTag<PublishResult> results) {
      return builder().setResultOutputTag(results).build();
    }

    @Override
    public PCollectionTuple expand(PCollection<PublishRequest> input) {
      checkArgument(getTopicName() != null, "withTopicName() is required");
      final PCollectionTuple tuple =
          input.apply(
              ParDo.of(new SnsWriterFn(this))
                  .withOutputTags(getResultOutputTag(), TupleTagList.empty()));
      return tuple;
    }

    private static class SnsWriterFn extends DoFn<PublishRequest, PublishResult> {

      private static final Logger LOG = LoggerFactory.getLogger(SnsWriterFn.class);
      private static final Counter SNS_WRITE_FAILURES =
          Metrics.counter(SnsWriterFn.class, "SNS_Write_Failures");

      private final SnsIO.Write spec;
      private transient AmazonSNS producer;
      private transient RetryPolicy retryPolicy;

      public SnsWriterFn(SnsIO.Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        producer = spec.getAwsClientsProvider().createSnsPublisher();
        retryPolicy =
            new RetryPolicy()
                .retryOn(ServiceUnavailableException.class)
                .withBackoff(
                    spec.getRetryDelay().toMillis(),
                    spec.getMaxDelay().toMillis(),
                    TimeUnit.MILLISECONDS)
                .withMaxRetries(spec.getMaxRetries());
        checkArgument(
            topicExists(producer, spec.getTopicName()),
            "Topic %s does not exist",
            spec.getTopicName());
      }

      private PublishResult getResult(PublishRequest request) throws Exception {
        PublishResult result = producer.publish(request);
        int statusCode = result.getSdkHttpMetadata().getHttpStatusCode();
        if (statusCode == 200) {
          return result;
        } else if (statusCode == 503) {
          throw new ServiceUnavailableException(
              "Service Temporarily unavailable. Will retry publishing " + request.getMessage(),
              new RuntimeException("Got " + statusCode));
        } else {
          throw new SnsWriteException(
              "Unable to publish message " + request.getMessage(),
              new RuntimeException("Got " + statusCode));
        }
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        PublishRequest request = context.element();
        Failsafe.with(retryPolicy)
            .run(
                () -> {
                  try {
                    PublishResult pr = getResult(request);
                    LOG.info(
                        "Published message {} with message id {} ",
                        request.getMessage(),
                        pr.getMessageId());
                    context.output(pr);
                  } catch (SnsWriteException | ServiceUnavailableException e) {
                    SNS_WRITE_FAILURES.inc();
                    LOG.error("Unable to publish message {} due to {} ", request.getMessage(), e);
                  }
                });
      }

      @Teardown
      public void tearDown() {
        if (producer != null) {
          producer.shutdown();
          producer = null;
        }
      }

      @SuppressWarnings({"checkstyle:illegalCatch"})
      private static boolean topicExists(AmazonSNS client, String topicName) {
        try {
          GetTopicAttributesResult topicAttributesResult = client.getTopicAttributes(topicName);
          return topicAttributesResult != null
              && topicAttributesResult.getSdkHttpMetadata().getHttpStatusCode() == 200;
        } catch (Exception e) {
          LOG.warn("Error checking whether topic {} exists.", topicName, e);
          throw e;
        }
      }
    }

    static class SnsWriteException extends IOException {
      SnsWriteException(String message, Throwable cause) {
        super(message, cause);
      }
    }

    static class ServiceUnavailableException extends IOException {
      ServiceUnavailableException(String message, Throwable cause) {
        super(message, cause);
      }
    }
  }
}
