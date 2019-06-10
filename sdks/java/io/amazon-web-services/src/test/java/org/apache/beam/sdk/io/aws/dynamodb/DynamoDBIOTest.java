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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

/** Unit Coverage for the IO. */
public class DynamoDBIOTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final transient ExpectedLogs expectedLogs = ExpectedLogs.none(DynamoDBIO.class);

  private static final String tableName = "TaskA";
  private static final int numOfItems = 10;

  private static List<Map<String, AttributeValue>> expected;

  @BeforeClass
  public static void setup() {
    DynamoDBIOTestHelper.startServerClient();
    DynamoDBIOTestHelper.createTestTable(tableName);
    expected = DynamoDBIOTestHelper.generateTestData(tableName, numOfItems);
  }

  @AfterClass
  public static void destroy() {
    DynamoDBIOTestHelper.stopServerClient(tableName);
  }

  // Test cases for Reader.
  @Ignore
  @Test
  public void testReadScanResult() {

    PCollection<List<Map<String, AttributeValue>>> actual =
        pipeline
            .apply(
                DynamoDBIO.<ScanResult>read()
                    .withScanRequestFn(v -> new ScanRequest(tableName).withTotalSegments(1))
                    .withRowMapper(new DynamoDBIOTestHelper.RowMapperTest())
                    .withCoder(SerializableCoder.of(ScanResult.class))
                    .withAwsClientsProvider(
                        AwsClientsProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient())))
            .apply(
                MapElements.via(
                    new SimpleFunction<ScanResult, List<Map<String, AttributeValue>>>() {
                      @Override
                      public List<Map<String, AttributeValue>> apply(ScanResult scanResult) {
                        return scanResult.getItems();
                      }
                    }));

    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  // Test cases for Reader's arguments.
  @Test
  public void testMissingScanRequestFn() {
    thrown.expectMessage("withScanRequestFn() is required");
    pipeline.apply(
        DynamoDBIO.read()
            .withAwsClientsProvider(
                AwsClientsProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient())));
    try {
      pipeline.run().waitUntilFinish();
      fail("withScanRequestFn() is required");
    } catch (IllegalArgumentException ex) {
      assertEquals("withScanRequestFn() is required", ex.getMessage());
    }
  }

  @Test
  public void testMissingAwsClientsProvider() {
    thrown.expectMessage("withAwsClientsProvider() is required");
    pipeline.apply(
        DynamoDBIO.read()
            .withScanRequestFn((v) -> new ScanRequest(tableName).withTotalSegments(3)));
    try {
      pipeline.run().waitUntilFinish();
      fail("withAwsClientsProvider() is required");
    } catch (IllegalArgumentException ex) {
      assertEquals("withAwsClientsProvider() is required", ex.getMessage());
    }
  }

  @Test
  public void testMissingTotalSegments() {
    thrown.expectMessage("TotalSegments is required with withScanRequestFn()");
    pipeline.apply(
        DynamoDBIO.read()
            .withScanRequestFn((v) -> new ScanRequest(tableName))
            .withAwsClientsProvider(
                AwsClientsProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient())));
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
            .withAwsClientsProvider(
                AwsClientsProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient())));
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
  public void testWriteDataToDynamo() {
    final BatchWriteItemRequest batchWriteItemRequest =
        DynamoDBIOTestHelper.generateBatchWriteItemRequest(tableName, numOfItems);

    final PCollection<BatchWriteItemResult> output =
        pipeline
            .apply(Create.of(batchWriteItemRequest))
            .apply(
                DynamoDBIO.write()
                    .withRetryConfiguration(
                        DynamoDBIO.RetryConfiguration.create(5, Duration.standardMinutes(1)))
                    .withAwsClientsProvider(
                        AwsClientsProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient())));

    final PCollection<Long> publishedResultsSize = output.apply(Count.globally());
    PAssert.that(publishedResultsSize).containsInAnyOrder(1L);

    pipeline.run().waitUntilFinish();
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testRetries() throws Throwable {
    thrown.expectMessage("Error writing to DynamoDB");
    final BatchWriteItemRequest batchWriteItemRequest =
        DynamoDBIOTestHelper.generateBatchWriteItemRequest(tableName, numOfItems);

    AmazonDynamoDB amazonDynamoDBMock = Mockito.mock(AmazonDynamoDB.class);
    Mockito.when(amazonDynamoDBMock.batchWriteItem(Mockito.any(BatchWriteItemRequest.class)))
        .thenThrow(new AmazonDynamoDBException("Service unavailable"));

    pipeline
        .apply(Create.of(batchWriteItemRequest))
        .apply(
            DynamoDBIO.write()
                .withRetryConfiguration(
                    DynamoDBIO.RetryConfiguration.create(4, Duration.standardSeconds(10)))
                .withAwsClientsProvider(AwsClientsProviderMock.of(amazonDynamoDBMock)));

    try {
      pipeline.run().waitUntilFinish();
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
