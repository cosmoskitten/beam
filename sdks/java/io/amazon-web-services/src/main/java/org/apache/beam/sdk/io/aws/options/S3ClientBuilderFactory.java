package org.apache.beam.sdk.io.aws.options;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;

/** Construct AmazonS3ClientBuilder from S3 pipeline options. */
public interface S3ClientBuilderFactory {
  AmazonS3ClientBuilder createBuilder(S3Options s3Options);
}
