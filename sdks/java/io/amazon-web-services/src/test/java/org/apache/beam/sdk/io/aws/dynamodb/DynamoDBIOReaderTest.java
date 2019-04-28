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
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/** Unit tests for Reader to cover different kind of parallel scan. */
public class DynamoDBIOReaderTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private AwsClientsProvider awsClientsProvider;
  private transient AmazonDynamoDB dynamoClient;

  private final String tableName = "Task";
  private final int numOfItemsToGen = 10;

  private List<Map<String, AttributeValue>> expected;

  @Before
  public void setup() {
    dynamoClient = DynamoDBEmbedded.create().amazonDynamoDB();
    awsClientsProvider = MockAwsClientProvider.of(dynamoClient);
    DynamoDBIOTestHelper.createTestTable(dynamoClient, tableName);
    expected = DynamoDBIOTestHelper.generateTestData(dynamoClient, tableName, numOfItemsToGen);
  }

  @Test
  public void testLimit10AndSplit1() {
    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withTableName(tableName)
                .withNumOfItemPerSegment(10)
                .withNumOfSplits(1)
                .withAWSClientsProvider(awsClientsProvider));

    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testLimit99WhileHasLess() {
    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withTableName(tableName)
                .withNumOfItemPerSegment(99)
                .withNumOfSplits(1)
                .withAWSClientsProvider(awsClientsProvider));

    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testLimit2WhileHasMore() {
    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withTableName(tableName)
                .withNumOfItemPerSegment(2)
                .withNumOfSplits(1)
                .withAWSClientsProvider(awsClientsProvider));

    PAssert.thatSingleton(actual.apply(Count.globally())).isEqualTo(2L);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testLimit2AndSplit2WhileHasMore() {
    final PCollection<Map<String, AttributeValue>> output =
        pipeline.apply(
            DynamoDBIO.read()
                .withTableName(tableName)
                .withNumOfItemPerSegment(2)
                .withNumOfSplits(2)
                .withAWSClientsProvider(awsClientsProvider));

    PAssert.thatSingleton(output.apply(Count.globally())).isEqualTo(4L);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testLimit2AndSplit5ReturnLessThan10() {
    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withTableName(tableName)
                .withNumOfItemPerSegment(2)
                .withNumOfSplits(5)
                .withAWSClientsProvider(awsClientsProvider));

    PAssert.thatSingleton(actual.apply(Count.globally())).notEqualTo(10L);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testLimit2AndSplit5() {
    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withTableName(tableName)
                // Don't set a limit when num of split is calculated. One segment can select more
                // item than your limit
                .withNumOfSplits(5)
                .withAWSClientsProvider(awsClientsProvider));

    PAssert.thatSingleton(actual.apply(Count.globally())).isEqualTo(10L);
    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testParallelWith12Split() {
    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withTableName(tableName)
                .withNumOfSplits(12)
                .withAWSClientsProvider(awsClientsProvider));
    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testParallelWith3Split() {
    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withTableName(tableName)
                .withNumOfSplits(3)
                .withAWSClientsProvider(awsClientsProvider));

    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testFilterWith2Split() {
    Map<String, AttributeValue> filterExpressionValues = new HashMap<>();
    filterExpressionValues.put(":number", new AttributeValue().withN("10005"));

    Map<String, String> filterExpressionNames = new HashMap<>();
    filterExpressionNames.put(DynamoDBIOTestHelper.ATTR_NAME_1, "HELLO");

    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withTableName(tableName)
                .withFilterExpression(DynamoDBIOTestHelper.ATTR_NAME_2 + " < :number")
                .withExpressionAttributeValues(filterExpressionValues)
                .withNumOfSplits(2)
                .withAWSClientsProvider(awsClientsProvider));

    PAssert.thatSingleton(actual.apply(Count.globally())).isEqualTo(4L);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testFilterAndProjectWith2Split() {
    List<Map<String, AttributeValue>> expectedFilter =
        ImmutableList.of(
            ImmutableMap.of(DynamoDBIOTestHelper.ATTR_NAME_2, new AttributeValue().withN("10001")),
            ImmutableMap.of(DynamoDBIOTestHelper.ATTR_NAME_2, new AttributeValue().withN("10002")),
            ImmutableMap.of(DynamoDBIOTestHelper.ATTR_NAME_2, new AttributeValue().withN("10003")),
            ImmutableMap.of(DynamoDBIOTestHelper.ATTR_NAME_2, new AttributeValue().withN("10004")));

    Map<String, AttributeValue> filterExpressionValues = new HashMap<>();
    filterExpressionValues.put(":number", new AttributeValue().withN("10005"));

    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withTableName(tableName)
                .withFilterExpression(DynamoDBIOTestHelper.ATTR_NAME_2 + " < :number")
                .withExpressionAttributeValues(filterExpressionValues)
                .withProjectionExpression(DynamoDBIOTestHelper.ATTR_NAME_2)
                .withNumOfSplits(2)
                .withAWSClientsProvider(awsClientsProvider));

    PAssert.that(actual).containsInAnyOrder(expectedFilter);
    pipeline.run().waitUntilFinish();
  }
}
