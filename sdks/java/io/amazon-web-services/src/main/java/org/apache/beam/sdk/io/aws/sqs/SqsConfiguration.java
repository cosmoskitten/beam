package org.apache.beam.sdk.io.aws.sqs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.io.aws.options.AwsModule;
import org.apache.beam.sdk.io.aws.options.AwsOptions;

class SqsConfiguration implements Serializable {

  private String awsRegion;
  private String awsCredentialsProviderString;
  private String awsClientConfigurationString;

  public SqsConfiguration(AwsOptions awsOptions) {
    ObjectMapper om = new ObjectMapper();
    om.registerModule(new AwsModule());
    try {
      this.awsCredentialsProviderString =
          om.writeValueAsString(awsOptions.getAwsCredentialsProvider());
    } catch (JsonProcessingException e) {
      this.awsCredentialsProviderString = null;
    }

    try {
      this.awsClientConfigurationString = om.writeValueAsString(awsOptions.getClientConfiguration());
    } catch (JsonProcessingException e) {
      this.awsClientConfigurationString = null;
    }

    this.awsRegion = awsOptions.getAwsRegion();
  }

  public AWSCredentialsProvider getAwsCredentialsProvider() {
    ObjectMapper om = new ObjectMapper();
    om.registerModule(new AwsModule());
    try {
      return om.readValue(awsCredentialsProviderString, AWSCredentialsProvider.class);
    } catch (IOException e) {
      return null;
    }
  }

  public ClientConfiguration getClientConfiguration() {
    ObjectMapper om = new ObjectMapper();
    om.registerModule(new AwsModule());
    try {
      return om.readValue(awsClientConfigurationString, ClientConfiguration.class);
    } catch (IOException e) {
      return null;
    }
  }

  public String getAwsRegion() {
    return awsRegion;
  }
}
