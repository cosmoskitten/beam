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

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateBackupRequest;
import com.amazonaws.services.dynamodbv2.model.CreateBackupResult;
import com.amazonaws.services.dynamodbv2.model.CreateGlobalTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateGlobalTableResult;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteBackupRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteBackupResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeBackupRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeBackupResult;
import com.amazonaws.services.dynamodbv2.model.DescribeContinuousBackupsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeContinuousBackupsResult;
import com.amazonaws.services.dynamodbv2.model.DescribeEndpointsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeEndpointsResult;
import com.amazonaws.services.dynamodbv2.model.DescribeGlobalTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeGlobalTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeGlobalTableSettingsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeGlobalTableSettingsResult;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTimeToLiveRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTimeToLiveResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ListBackupsRequest;
import com.amazonaws.services.dynamodbv2.model.ListBackupsResult;
import com.amazonaws.services.dynamodbv2.model.ListGlobalTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListGlobalTablesResult;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ListTagsOfResourceRequest;
import com.amazonaws.services.dynamodbv2.model.ListTagsOfResourceResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.RestoreTableFromBackupRequest;
import com.amazonaws.services.dynamodbv2.model.RestoreTableFromBackupResult;
import com.amazonaws.services.dynamodbv2.model.RestoreTableToPointInTimeRequest;
import com.amazonaws.services.dynamodbv2.model.RestoreTableToPointInTimeResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TagResourceRequest;
import com.amazonaws.services.dynamodbv2.model.TagResourceResult;
import com.amazonaws.services.dynamodbv2.model.TransactGetItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactGetItemsResult;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import com.amazonaws.services.dynamodbv2.model.UntagResourceRequest;
import com.amazonaws.services.dynamodbv2.model.UntagResourceResult;
import com.amazonaws.services.dynamodbv2.model.UpdateContinuousBackupsRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateContinuousBackupsResult;
import com.amazonaws.services.dynamodbv2.model.UpdateGlobalTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateGlobalTableResult;
import com.amazonaws.services.dynamodbv2.model.UpdateGlobalTableSettingsRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateGlobalTableSettingsResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.waiters.AmazonDynamoDBWaiters;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** Mock AmazonDynamoDB for unit test. */
public abstract class AmazonDynamoDBMock implements AmazonDynamoDB, Serializable {

  @Override
  public void setEndpoint(String endpoint) {}

  @Override
  public void setRegion(Region region) {}

  @Override
  public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest) {
    return null;
  }

  @Override
  public BatchGetItemResult batchGetItem(
      Map<String, KeysAndAttributes> requestItems, String returnConsumedCapacity) {
    return null;
  }

  @Override
  public BatchGetItemResult batchGetItem(Map<String, KeysAndAttributes> requestItems) {
    return null;
  }

  @Override
  public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequest) {
    return null;
  }

  @Override
  public BatchWriteItemResult batchWriteItem(Map<String, List<WriteRequest>> requestItems) {
    return null;
  }

  @Override
  public CreateBackupResult createBackup(CreateBackupRequest createBackupRequest) {
    return null;
  }

  @Override
  public CreateGlobalTableResult createGlobalTable(
      CreateGlobalTableRequest createGlobalTableRequest) {
    return null;
  }

  @Override
  public CreateTableResult createTable(CreateTableRequest createTableRequest) {
    return null;
  }

  @Override
  public CreateTableResult createTable(
      List<AttributeDefinition> attributeDefinitions,
      String tableName,
      List<KeySchemaElement> keySchema,
      ProvisionedThroughput provisionedThroughput) {
    return null;
  }

  @Override
  public DeleteBackupResult deleteBackup(DeleteBackupRequest deleteBackupRequest) {
    return null;
  }

  @Override
  public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest) {
    return null;
  }

  @Override
  public DeleteItemResult deleteItem(String tableName, Map<String, AttributeValue> key) {
    return null;
  }

  @Override
  public DeleteItemResult deleteItem(
      String tableName, Map<String, AttributeValue> key, String returnValues) {
    return null;
  }

  @Override
  public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) {
    return null;
  }

  @Override
  public DeleteTableResult deleteTable(String tableName) {
    return null;
  }

  @Override
  public DescribeBackupResult describeBackup(DescribeBackupRequest describeBackupRequest) {
    return null;
  }

  @Override
  public DescribeContinuousBackupsResult describeContinuousBackups(
      DescribeContinuousBackupsRequest describeContinuousBackupsRequest) {
    return null;
  }

  @Override
  public DescribeEndpointsResult describeEndpoints(
      DescribeEndpointsRequest describeEndpointsRequest) {
    return null;
  }

  @Override
  public DescribeGlobalTableResult describeGlobalTable(
      DescribeGlobalTableRequest describeGlobalTableRequest) {
    return null;
  }

  @Override
  public DescribeGlobalTableSettingsResult describeGlobalTableSettings(
      DescribeGlobalTableSettingsRequest describeGlobalTableSettingsRequest) {
    return null;
  }

  @Override
  public DescribeLimitsResult describeLimits(DescribeLimitsRequest describeLimitsRequest) {
    return null;
  }

  @Override
  public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) {
    return null;
  }

  @Override
  public DescribeTableResult describeTable(String tableName) {
    return null;
  }

  @Override
  public DescribeTimeToLiveResult describeTimeToLive(
      DescribeTimeToLiveRequest describeTimeToLiveRequest) {
    return null;
  }

  @Override
  public GetItemResult getItem(GetItemRequest getItemRequest) {
    return null;
  }

  @Override
  public GetItemResult getItem(String tableName, Map<String, AttributeValue> key) {
    return null;
  }

  @Override
  public GetItemResult getItem(
      String tableName, Map<String, AttributeValue> key, Boolean consistentRead) {
    return null;
  }

  @Override
  public ListBackupsResult listBackups(ListBackupsRequest listBackupsRequest) {
    return null;
  }

  @Override
  public ListGlobalTablesResult listGlobalTables(ListGlobalTablesRequest listGlobalTablesRequest) {
    return null;
  }

  @Override
  public ListTablesResult listTables(ListTablesRequest listTablesRequest) {
    return null;
  }

  @Override
  public ListTablesResult listTables() {
    return null;
  }

  @Override
  public ListTablesResult listTables(String exclusiveStartTableName) {
    return null;
  }

  @Override
  public ListTablesResult listTables(String exclusiveStartTableName, Integer limit) {
    return null;
  }

  @Override
  public ListTablesResult listTables(Integer limit) {
    return null;
  }

  @Override
  public ListTagsOfResourceResult listTagsOfResource(
      ListTagsOfResourceRequest listTagsOfResourceRequest) {
    return null;
  }

  @Override
  public PutItemResult putItem(PutItemRequest putItemRequest) {
    return null;
  }

  @Override
  public PutItemResult putItem(String tableName, Map<String, AttributeValue> item) {
    return null;
  }

  @Override
  public PutItemResult putItem(
      String tableName, Map<String, AttributeValue> item, String returnValues) {
    return null;
  }

  @Override
  public QueryResult query(QueryRequest queryRequest) {
    return null;
  }

  @Override
  public RestoreTableFromBackupResult restoreTableFromBackup(
      RestoreTableFromBackupRequest restoreTableFromBackupRequest) {
    return null;
  }

  @Override
  public RestoreTableToPointInTimeResult restoreTableToPointInTime(
      RestoreTableToPointInTimeRequest restoreTableToPointInTimeRequest) {
    return null;
  }

  @Override
  public ScanResult scan(ScanRequest scanRequest) {
    return null;
  }

  @Override
  public ScanResult scan(String tableName, List<String> attributesToGet) {
    return null;
  }

  @Override
  public ScanResult scan(String tableName, Map<String, Condition> scanFilter) {
    return null;
  }

  @Override
  public ScanResult scan(
      String tableName, List<String> attributesToGet, Map<String, Condition> scanFilter) {
    return null;
  }

  @Override
  public TagResourceResult tagResource(TagResourceRequest tagResourceRequest) {
    return null;
  }

  @Override
  public TransactGetItemsResult transactGetItems(TransactGetItemsRequest transactGetItemsRequest) {
    return null;
  }

  @Override
  public TransactWriteItemsResult transactWriteItems(
      TransactWriteItemsRequest transactWriteItemsRequest) {
    return null;
  }

  @Override
  public UntagResourceResult untagResource(UntagResourceRequest untagResourceRequest) {
    return null;
  }

  @Override
  public UpdateContinuousBackupsResult updateContinuousBackups(
      UpdateContinuousBackupsRequest updateContinuousBackupsRequest) {
    return null;
  }

  @Override
  public UpdateGlobalTableResult updateGlobalTable(
      UpdateGlobalTableRequest updateGlobalTableRequest) {
    return null;
  }

  @Override
  public UpdateGlobalTableSettingsResult updateGlobalTableSettings(
      UpdateGlobalTableSettingsRequest updateGlobalTableSettingsRequest) {
    return null;
  }

  @Override
  public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest) {
    return null;
  }

  @Override
  public UpdateItemResult updateItem(
      String tableName,
      Map<String, AttributeValue> key,
      Map<String, AttributeValueUpdate> attributeUpdates) {
    return null;
  }

  @Override
  public UpdateItemResult updateItem(
      String tableName,
      Map<String, AttributeValue> key,
      Map<String, AttributeValueUpdate> attributeUpdates,
      String returnValues) {
    return null;
  }

  @Override
  public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest) {
    return null;
  }

  @Override
  public UpdateTableResult updateTable(
      String tableName, ProvisionedThroughput provisionedThroughput) {
    return null;
  }

  @Override
  public UpdateTimeToLiveResult updateTimeToLive(UpdateTimeToLiveRequest updateTimeToLiveRequest) {
    return null;
  }

  @Override
  public void shutdown() {}

  @Override
  public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
    return null;
  }

  @Override
  public AmazonDynamoDBWaiters waiters() {
    return null;
  }
}
