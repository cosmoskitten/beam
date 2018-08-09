package org.apache.beam.sdk.io.sns;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;

/**
 * Provides default provider implementation for SnS.
 *
 * <p>Please note, that any instance of {@link AwsClientsProvider} must be {@link
 * java.io.Serializable} to ensure it can be sent to worker machines.
 */
public class DefaultSnsClientProvider implements AwsClientsProvider {

  @Override
  public AmazonCloudWatch getCloudWatchClient() {
    return AmazonCloudWatchClientBuilder.defaultClient();
  }

  @Override
  public AmazonSNS createSnsPublisher() {
    return AmazonSNSClientBuilder.defaultClient();
  }
}
