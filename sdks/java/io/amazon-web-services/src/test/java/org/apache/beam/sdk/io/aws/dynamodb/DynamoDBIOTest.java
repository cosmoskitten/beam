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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit Coverage for the IO. */
public class DynamoDBIOTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final transient ExpectedLogs expectedLogs = ExpectedLogs.none(DynamoDBIO.class);

  private static DynamoDBIO.DynamoDBConfiguration config;

  private static final String tableName = "TaskA";
  private static final int numOfItems = 10;

  private static List<Map<String, AttributeValue>> expected;

  @BeforeClass
  public static void setup() {
    DynamoDBIOTestHelper.startServerClient();
    DynamoDBIOTestHelper.createTestTable(tableName);
    expected = DynamoDBIOTestHelper.generateTestData(tableName, numOfItems);

    config = DynamoDBIOTestHelper.getDynamoDBConfig();
  }

  @AfterClass
  public static void destroy() {
    DynamoDBIOTestHelper.stopServerClient(tableName);
  }

  // Test cases for Reader.
  @Test
  public void testLimit10AndSplit1() {
    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withScanRequestFn(
                    (v) -> new ScanRequest(tableName).withLimit(10).withTotalSegments(1))
                .withDynamoDBConfiguration(config));

    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testLimit99WhileHasLess() {
    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withScanRequestFn(
                    (v) -> new ScanRequest(tableName).withLimit(99).withTotalSegments(1))
                .withDynamoDBConfiguration(config));

    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testLimit2WhileHasMore() {
    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withScanRequestFn(
                    (v) -> new ScanRequest(tableName).withLimit(2).withTotalSegments(1))
                .withDynamoDBConfiguration(config));

    PAssert.thatSingleton(actual.apply(Count.globally())).isEqualTo(2L);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testLimit2AndSplit2WhileHasMore() {
    final PCollection<Map<String, AttributeValue>> output =
        pipeline.apply(
            DynamoDBIO.read()
                .withScanRequestFn(
                    (v) -> new ScanRequest(tableName).withLimit(2).withTotalSegments(2))
                .withDynamoDBConfiguration(config));

    PAssert.thatSingleton(output.apply(Count.globally())).isEqualTo(4L);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testLimit2AndSplit5ReturnLessThan10() {
    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withScanRequestFn(
                    (v) -> new ScanRequest(tableName).withLimit(2).withTotalSegments(5))
                .withDynamoDBConfiguration(config));

    PAssert.thatSingleton(actual.apply(Count.globally())).notEqualTo(10L);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testLimit2AndSplit5() {
    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withScanRequestFn((v) -> new ScanRequest(tableName).withTotalSegments(5))
                // Don't set a limit when num of split is calculated. One segment can select more
                // item than your limit
                .withDynamoDBConfiguration(config));

    PAssert.thatSingleton(actual.apply(Count.globally())).isEqualTo(10L);
    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testParallelWith12Split() {
    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withScanRequestFn((v) -> new ScanRequest(tableName).withTotalSegments(12))
                .withDynamoDBConfiguration(config));
    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testParallelWith3Split() {
    final PCollection<Map<String, AttributeValue>> actual =
        pipeline.apply(
            DynamoDBIO.read()
                .withScanRequestFn((v) -> new ScanRequest(tableName).withTotalSegments(3))
                .withDynamoDBConfiguration(config));

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
                .withScanRequestFn(
                    (v) ->
                        new ScanRequest(tableName)
                            .withTotalSegments(2)
                            .withFilterExpression(DynamoDBIOTestHelper.ATTR_NAME_2 + " < :number")
                            .withExpressionAttributeValues(filterExpressionValues))
                .withDynamoDBConfiguration(config));

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
                .withScanRequestFn(
                    (v) ->
                        new ScanRequest(tableName)
                            .withTotalSegments(2)
                            .withFilterExpression(DynamoDBIOTestHelper.ATTR_NAME_2 + " < :number")
                            .withExpressionAttributeValues(filterExpressionValues)
                            .withProjectionExpression(DynamoDBIOTestHelper.ATTR_NAME_2))
                .withDynamoDBConfiguration(config));

    PAssert.that(actual).containsInAnyOrder(expectedFilter);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteDataToDynamo() {
    final BatchWriteItemRequest batchWriteItemRequest =
        DynamoDBIOTestHelper.generateBatchWriteItemRequest(tableName, numOfItems);

    final TupleTag<BatchWriteItemResult> results = new TupleTag<>();

    final PCollectionTuple output =
        pipeline
            .apply(Create.of(batchWriteItemRequest))
            .apply(
                DynamoDBIO.write()
                    .withRetryConfiguration(
                        DynamoDBIO.RetryConfiguration.create(5, Duration.standardMinutes(1)))
                    .withDynamoDBConfiguration(config)
                    .withResultOutputTag(results));

    final PCollection<Long> publishedResultsSize = output.get(results).apply(Count.globally());
    PAssert.that(publishedResultsSize).containsInAnyOrder(1L);

    pipeline.run().waitUntilFinish();
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  // Test cases for Reader's arguments.
  @Test
  public void testMissingScanRequestFn() {
    thrown.expectMessage("withScanRequestFn() is required");
    pipeline.apply(DynamoDBIO.read().withDynamoDBConfiguration(config));
    try {
      pipeline.run().waitUntilFinish();
      fail("withScanRequestFn() is required");
    } catch (IllegalArgumentException ex) {
      assertEquals("withScanRequestFn() is required", ex.getMessage());
    }
  }

  @Test
  public void testMissingDynamodbConfiguration() {
    thrown.expectMessage("withDynamoDBConfiguration() is required");
    pipeline.apply(
        DynamoDBIO.read()
            .withScanRequestFn((v) -> new ScanRequest(tableName).withTotalSegments(3)));
    try {
      pipeline.run().waitUntilFinish();
      fail("withDynamoDBConfiguration() is required");
    } catch (IllegalArgumentException ex) {
      assertEquals("withDynamoDBConfiguration() is required", ex.getMessage());
    }
  }

  @Test
  public void testMissingTotalSegments() {
    thrown.expectMessage("TotalSegments is required with withScanRequestFn()");
    pipeline.apply(
        DynamoDBIO.read()
            .withScanRequestFn((v) -> new ScanRequest(tableName))
            .withDynamoDBConfiguration(config));
    try {
      pipeline.run().waitUntilFinish();
      fail("TotalSegments is required with withScanRequestFn()");
    } catch (IllegalArgumentException ex) {
      assertEquals("TotalSegments is required with withScanRequestFn()", ex.getMessage());
    }
  }

  @Test
  public void testNegativeTotalSegments() {
    thrown.expectMessage("TotalSegments is required with withScanRequestFn() and greater zero");
    pipeline.apply(
        DynamoDBIO.read()
            .withScanRequestFn((v) -> new ScanRequest(tableName).withTotalSegments(-1))
            .withDynamoDBConfiguration(config));
    try {
      pipeline.run().waitUntilFinish();
      fail("withTotalSegments() is expected and greater than zero");
    } catch (IllegalArgumentException ex) {
      assertEquals(
          "TotalSegments is required with withScanRequestFn() and greater zero", ex.getMessage());
    }
  }

  // Test cases for Writer.
  @Test
  public void testRetries() throws Throwable {
    thrown.expectMessage("Error writing to DynamoDB");
    final BatchWriteItemRequest batchWriteItemRequest =
        DynamoDBIOTestHelper.generateBatchWriteItemRequest(tableName, numOfItems);

    final TupleTag<BatchWriteItemResult> results = new TupleTag<>();
    pipeline
        .apply(Create.of(batchWriteItemRequest))
        .apply(
            DynamoDBIO.write()
                .withRetryConfiguration(
                    DynamoDBIO.RetryConfiguration.create(4, Duration.standardSeconds(10)))
                .withDynamoDBConfiguration(new DynamoDBConfigMock())
                .withResultOutputTag(results));

    try {
      pipeline.run();
    } catch (final Pipeline.PipelineExecutionException e) {
      // check 3 retries were initiated by inspecting the log before passing on the exception
      expectedLogs.verifyWarn(
          String.format(DynamoDBIO.Write.DynamoDbWriterFn.RETRY_ATTEMPT_LOG, 1));
      expectedLogs.verifyWarn(
          String.format(DynamoDBIO.Write.DynamoDbWriterFn.RETRY_ATTEMPT_LOG, 2));
      expectedLogs.verifyWarn(
          String.format(DynamoDBIO.Write.DynamoDbWriterFn.RETRY_ATTEMPT_LOG, 3));
      throw e.getCause();
    }
    fail("Pipeline is expected to fail because we were unable to write to DynamoDB.");
  }
}
