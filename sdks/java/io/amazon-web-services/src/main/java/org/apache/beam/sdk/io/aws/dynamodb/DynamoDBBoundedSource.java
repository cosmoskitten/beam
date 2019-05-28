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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Suppliers;

/**
 * Provide BoundedSource instance for {@link DynamoDBIO}, and parameters used by {@link
 * DynamoDBBoundedReader}.
 */
@VisibleForTesting
class DynamoDBBoundedSource extends BoundedSource<Map<String, AttributeValue>> {

  private final DynamoDBIO.Read read;
  private final int segmentId;
  private final Supplier<AmazonDynamoDB> client;

  DynamoDBBoundedSource(DynamoDBIO.Read read, int segmentId) {
    this.read = read;
    this.segmentId = segmentId;
    client =
        Suppliers.memoize(
            (Supplier<AmazonDynamoDB> & Serializable)
                () -> Objects.requireNonNull(read.getAwsClientsProvider()).createDynamoDB());
  }

  @Override
  public Coder<Map<String, AttributeValue>> getOutputCoder() {
    return MapCoder.of(StringUtf8Coder.of(), AttributeValueCoder.of());
  }

  @Override
  public List<? extends BoundedSource<Map<String, AttributeValue>>> split(
      long desiredBundleSizeBytes, PipelineOptions options) {
    List<DynamoDBBoundedSource> sources = new ArrayList<>();
    int numOfSplits =
        Objects.requireNonNull(read.getScanRequestFn()).apply(null).getTotalSegments();
    for (int i = 0; i < numOfSplits; i++) {
      sources.add(new DynamoDBBoundedSource(read, i));
    }

    return sources;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) {
    // Dynamo does not seem to expose table sizes
    return 0;
  }

  @Override
  public BoundedReader<Map<String, AttributeValue>> createReader(PipelineOptions options) {
    return new DynamoDBBoundedReader(this);
  }

  public DynamoDBIO.Read getRead() {
    return read;
  }

  int getSegmentId() {
    return segmentId;
  }

  public AmazonDynamoDB getClient() {
    return client.get();
  }
}
