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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;

/**
 * Provide BoundedSource instance for {@link DynamodbIO}, and parameters used by {@link
 * DynamodbBoundedReader}.
 */
@VisibleForTesting
public class DynamodbBoundedSource extends BoundedSource<Map<String, AttributeValue>> {

  final AwsClientsProvider awsClientsProvider;
  final String tableName;
  final String filterExpression;
  final Map<String, String> filterExpressionMapName;
  final Map<String, AttributeValue> filterExpressionMapValue;
  final String projectionExpression;
  final int numOfItemPerSegment;
  final int segmentId;
  final int totalSegment;

  public DynamodbBoundedSource(
      AwsClientsProvider awsClientsProvider,
      String tableName,
      String filterExpression,
      Map<String, String> filterExpressionMapName,
      Map<String, AttributeValue> filterExpressionMapValue,
      String projectionExpression,
      int numOfItemPerSegment,
      int totalSegment,
      int segmentId) {
    this.awsClientsProvider = awsClientsProvider;
    this.tableName = tableName;
    this.filterExpression = filterExpression;
    this.filterExpressionMapName = filterExpressionMapName;
    this.filterExpressionMapValue = filterExpressionMapValue;
    this.projectionExpression = projectionExpression;
    this.numOfItemPerSegment = numOfItemPerSegment;
    this.totalSegment = totalSegment;
    this.segmentId = segmentId;
  }

  @Override
  public Coder<Map<String, AttributeValue>> getOutputCoder() {
    return MapCoder.of(StringUtf8Coder.of(), AttributeValueCoder.of());
  }

  @Override
  public List<? extends BoundedSource<Map<String, AttributeValue>>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    List<DynamodbBoundedSource> sources = new ArrayList<>();
    for (int i = 0; i < totalSegment; i++) {
      sources.add(
          new DynamodbBoundedSource(
              awsClientsProvider,
              tableName,
              filterExpression,
              filterExpressionMapName,
              filterExpressionMapValue,
              projectionExpression,
              numOfItemPerSegment,
              totalSegment,
              i));
    }

    return sources;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    // Dynamo does not seem to expose table sizes
    return 0;
  }

  @Override
  public BoundedReader<Map<String, AttributeValue>> createReader(PipelineOptions options)
      throws IOException {
    return new DynamodbBoundedReader(this);
  }
}
