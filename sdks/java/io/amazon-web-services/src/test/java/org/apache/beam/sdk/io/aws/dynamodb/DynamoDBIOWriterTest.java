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

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for Writer to cover the write and retry functionality. */
public class DynamoDBIOWriterTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final transient ExpectedLogs expectedLogs = ExpectedLogs.none(DynamoDBIO.class);

  private AwsClientsProvider awsClientsProvider;
  private transient AmazonDynamoDB dynamoClient;

  private final String tableName = "Task";
  private final int numOfItemsToGen = 10;

  @Before
  public void setup() {
    dynamoClient = DynamoDBEmbedded.create().amazonDynamoDB();
    awsClientsProvider = MockAwsClientProvider.of(dynamoClient);
    DynamoDBIOTestHelper.createTestTable(dynamoClient, tableName);
  }

  @Test
  public void testWriteDataToDynamo() {
    final BatchWriteItemRequest batchWriteItemRequest =
        DynamoDBIOTestHelper.generateBatchWriteItemRequest(tableName, numOfItemsToGen);

    final TupleTag<BatchWriteItemResult> results = new TupleTag<>();

    final PCollectionTuple output =
        pipeline
            .apply(Create.of(batchWriteItemRequest))
            .apply(
                DynamoDBIO.write()
                    .withRetryConfiguration(
                        DynamoDBIO.RetryConfiguration.create(5, Duration.standardMinutes(1)))
                    .withAWSClientsProvider(awsClientsProvider)
                    .withResultOutputTag(results));

    final PCollection<Long> publishedResultsSize = output.get(results).apply(Count.globally());
    PAssert.that(publishedResultsSize).containsInAnyOrder(Long.valueOf(1L));

    pipeline.run().waitUntilFinish();
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testRetries() throws Throwable {
    thrown.expectMessage("Error writing to DynamoDB");
    final BatchWriteItemRequest batchWriteItemRequest =
        DynamoDBIOTestHelper.generateBatchWriteItemRequest(tableName, numOfItemsToGen);

    AmazonDynamoDB dynamoMock = mock(AmazonDynamoDB.class);
    when(dynamoMock.batchWriteItem(batchWriteItemRequest))
        .thenThrow(new AmazonDynamoDBException("Service unavailable"));

    final TupleTag<BatchWriteItemResult> results = new TupleTag<>();
    pipeline
        .apply(Create.of(batchWriteItemRequest))
        .apply(
            DynamoDBIO.write()
                .withRetryConfiguration(
                    DynamoDBIO.RetryConfiguration.create(4, Duration.standardSeconds(10)))
                .withAWSClientsProvider(MockAwsClientProvider.of(dynamoMock))
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
