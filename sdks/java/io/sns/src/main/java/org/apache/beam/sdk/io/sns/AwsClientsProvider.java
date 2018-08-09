package org.apache.beam.sdk.io.sns;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.sns.AmazonSNS;
import java.io.Serializable;

/**
 * Provides instances of AWS clients.
 *
 * <p>Please note, that any instance of {@link AwsClientsProvider} must be {@link Serializable} to
 * ensure it can be sent to worker machines.
 */
public interface AwsClientsProvider extends Serializable {
  AmazonCloudWatch getCloudWatchClient();

  AmazonSNS createSnsPublisher();
}
