package org.apache.beam.sdk.io.aws.s3;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.base.Strings;
import org.apache.beam.sdk.io.aws.options.S3ClientBuilderFactory;
import org.apache.beam.sdk.io.aws.options.S3Options;

/**
 * Construct AmazonS3ClientBuilder with default values of S3 client properties like path style
 * access, accelerated mode, etc.
 */
public class DefaultS3ClientBuilderFactory implements S3ClientBuilderFactory {

  @Override
  public AmazonS3ClientBuilder createBuilder(S3Options s3Options) {
    AmazonS3ClientBuilder builder =
        AmazonS3ClientBuilder.standard().withCredentials(s3Options.getAwsCredentialsProvider());

    if (s3Options.getClientConfiguration() != null) {
      builder = builder.withClientConfiguration(s3Options.getClientConfiguration());
    }

    if (Strings.isNullOrEmpty(s3Options.getAwsServiceEndpoint())) {
      builder = builder.withRegion(s3Options.getAwsRegion());
    } else {
      builder =
          builder.withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(
                  s3Options.getAwsServiceEndpoint(), s3Options.getAwsRegion()));
    }
    return builder;
  }
}
