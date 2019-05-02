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
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.ValueProvider;

/** Mock DynamoDBConfiguration to pass it over the write to writer for testing Writer's retries. */
public class DynamoDBConfigMock extends DynamoDBIO.DynamoDBConfiguration {
  @Override
  AmazonDynamoDB buildAmazonDynamoDB() {
    return new AmazonDynamoDBMockErrors();
  }

  @Nullable
  @Override
  ValueProvider<String> getRegion() {
    return null;
  }

  @Nullable
  @Override
  ValueProvider<String> getEndpointUrl() {
    return null;
  }

  @Nullable
  @Override
  ValueProvider<String> getAwsAccessKey() {
    return null;
  }

  @Nullable
  @Override
  ValueProvider<String> getAwsSecretKey() {
    return null;
  }

  @Override
  Builder builder() {
    return null;
  }
}
