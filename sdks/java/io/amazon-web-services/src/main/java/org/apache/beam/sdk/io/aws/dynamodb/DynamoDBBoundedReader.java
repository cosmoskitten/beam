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
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.beam.sdk.io.BoundedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider reader for scanning or reading data from DynamoDb. It will scan your table in parallel
 * base of totalScannedItemCount and segmentId arguments from {@link DynamoDBBoundedSource}.
 */
class DynamoDBBoundedReader extends BoundedSource.BoundedReader<Map<String, AttributeValue>> {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoDBBoundedReader.class);

  private DynamoDBBoundedSource source;

  private Iterator<Map<String, AttributeValue>> iter;
  private Map<String, AttributeValue> current;
  private long totalScannedItemCount;

  DynamoDBBoundedReader(DynamoDBBoundedSource source) {
    this.source = source;
  }

  @Override
  public boolean start() {
    ScanRequest scanRequest =
        Objects.requireNonNull(source.getRead().getScanRequestFn()).apply(null);
    scanRequest.setSegment(source.getSegmentId());

    ScanResult result = source.getClient().scan(scanRequest);
    if (result == null) {
      return false;
    }
    iter = result.getItems().iterator();

    return advance();
  }

  @Override
  public boolean advance() {
    if (iter != null && iter.hasNext()) {
      current = iter.next();
      totalScannedItemCount++;
      return true;
    }
    return false;
  }

  @Override
  public Map<String, AttributeValue> getCurrent() throws NoSuchElementException {
    return current;
  }

  @Override
  public void close() {
    LOG.debug(
        "Closing reader id {} after reading {} records.",
        source.getSegmentId(),
        totalScannedItemCount);
  }

  @Override
  public BoundedSource<Map<String, AttributeValue>> getCurrentSource() {
    return source;
  }
}
