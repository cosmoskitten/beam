package org.apache.beam.sdk.io.sns;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.time.Duration;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests to verify writes to Sns. */
@RunWith(JUnit4.class)
public class SnsIOTest implements Serializable {

  private static final String topicName = "arn:aws:sns:us-west-2:5880:topic-FMFEHJ47NRFO";

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static PublishRequest createSampleMessage(String message) {
    PublishRequest request = new PublishRequest().withTopicArn(topicName).withMessage(message);
    return request;
  }

  private static class Provider implements AwsClientsProvider {

    private static AmazonSNS publisher;

    public Provider(AmazonSNS pub) {
      publisher = pub;
    }

    @Override
    public AmazonCloudWatch getCloudWatchClient() {
      return Mockito.mock(AmazonCloudWatch.class);
    }

    @Override
    public AmazonSNS createSnsPublisher() {
      return publisher;
    }
  }

  @Test
  public void testDataWritesToSNS() {
    final PublishRequest request1 = createSampleMessage("my_first_message");
    final PublishRequest request2 = createSampleMessage("my_second_message");
    p.getCoderRegistry().registerCoderForClass(PublishResult.class, new PublishResultCoder());

    final TupleTag<PublishResult> results = new TupleTag<>();

    final PCollectionTuple snsWrites =
        p.apply(Create.of(request1, request2))
            .apply(
                SnsIO.write()
                    .withTopicName(topicName)
                    .withMaxRetries(2)
                    .withMaxDelay(Duration.ofSeconds(2))
                    .withRetryDelay(Duration.ofSeconds(1))
                    .withAwsClientsProvider(new Provider(new AmazonSNSMock()))
                    .withResultOutputTag(results));

    final PCollection<Long> publishedResultsSize = snsWrites.get(results).apply(Count.globally());
    PAssert.that(publishedResultsSize).containsInAnyOrder(ImmutableList.of(2L));
    p.run().waitUntilFinish();
  }
}
