package org.apache.beam.sdk.io.aws.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Options used to configure Amazon Web Services specific options such as credentials and region.
 */
public interface AwsOptions extends PipelineOptions {
  @Description("AWS access key ID")
  String getAwsAccessKeyId();
  void setAwsAccessKeyId(String value);

  @Description("AWS secret access key")
  String getAwsSecretAccessKey();
  void setAwsSecretAccessKey(String value);

  @Description("AWS region used by the AWS client")
  String getAwsRegion();
  void setAwsRegion(String value);
}
