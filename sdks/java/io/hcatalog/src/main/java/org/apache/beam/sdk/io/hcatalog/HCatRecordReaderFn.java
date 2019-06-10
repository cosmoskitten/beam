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
package org.apache.beam.sdk.io.hcatalog;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.hcatalog.HCatalogIO.Read;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unbounded reader for doing reads from Hcat. */
@BoundedPerElement
class HCatRecordReaderFn extends DoFn<Read, HCatRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(HCatRecordReaderFn.class);

  private ReaderContext getReaderContext(Read readRequest, long desiredSplitCount)
      throws HCatException {
    final Partition partition = readRequest.getPartitionToRead();
    final List<String> values = partition.getValues();
    final ImmutableList<String> partitionCols = readRequest.getPartitionCols();
    checkArgument(
        values.size() == partitionCols.size(),
        "Number of input partitions should be equal to the values of list partition values.");

    List<String> filter = new ArrayList<>();
    for (int i = 0; i < partitionCols.size(); i++) {
      filter.add(partitionCols.get(i) + "=" + "'" + values.get(i) + "'");
    }
    final String filterString = String.join(" and ", filter);

    ReadEntity entity =
        new ReadEntity.Builder()
            .withDatabase(readRequest.getDatabase())
            .withFilter(filterString)
            .withTable(readRequest.getTable())
            .build();
    // pass the 'desired' split count as an hint to the API
    Map<String, String> configProps = new HashMap<>(readRequest.getConfigProperties());
    configProps.put(
        HCatConstants.HCAT_DESIRED_PARTITION_NUM_SPLITS, String.valueOf(desiredSplitCount));
    return DataTransferFactory.getHCatReader(entity, configProps).prepareRead();
  }

  private long getFileSizeForPartitions(Read readRequest) throws Exception {
    IMetaStoreClient client = null;
    try {
      HiveConf hiveConf = HCatalogUtils.createHiveConf(readRequest);
      client = HCatalogUtils.createMetaStoreClient(hiveConf);
      List<org.apache.hadoop.hive.ql.metadata.Partition> p = new ArrayList<>();
      Table table = HCatUtil.getTable(client, readRequest.getDatabase(), readRequest.getTable());
      final org.apache.hadoop.hive.ql.metadata.Partition partition =
          new org.apache.hadoop.hive.ql.metadata.Partition(table, readRequest.getPartitionToRead());
      p.add(partition);
      final List<Long> fileSizeForPartitions = StatsUtils.getFileSizeForPartitions(hiveConf, p);
      return fileSizeForPartitions.get(0);
    } finally {
      // IMetaStoreClient is not AutoCloseable, closing it manually
      if (client != null) {
        client.close();
      }
    }
  }

  /** Reads data for a specific partition. */
  @ProcessElement
  @SuppressWarnings("unused")
  public void processElement(
      ProcessContext processContext, RestrictionTracker<OffsetRange, Long> tracker)
      throws Exception {
    final Read readRequest = processContext.element();
    final List<String> values = readRequest.getPartitionToRead().getValues();
    final String watermarkPartitionColumn = readRequest.getWatermarkPartitionColumn();
    final int indexOfPartitionColumnWithWaterMark =
        watermarkPartitionColumn.indexOf(watermarkPartitionColumn);
    final String partitionWaterMark = values.get(indexOfPartitionColumnWithWaterMark);

    final Instant partitionWatermarkTimeStamp =
        readRequest.getWatermarkTimestampConverter().apply(partitionWaterMark);

    int desiredSplitCount = 1;
    long estimatedSizeBytes = getFileSizeForPartitions(processContext.element());
    if (estimatedSizeBytes > 0) {
      desiredSplitCount = (int) Math.ceil((double) estimatedSizeBytes / Integer.MAX_VALUE);
    }
    ReaderContext readerContext = getReaderContext(processContext.element(), desiredSplitCount);

    for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); i++) {
      HCatReader reader = DataTransferFactory.getHCatReader(readerContext, (int) i);
      Iterator<HCatRecord> hcatIterator = reader.read();
      while (hcatIterator.hasNext()) {
        final HCatRecord record = hcatIterator.next();
        processContext.output(record);
      }
    }
    LOG.info("Watermark update to {}", partitionWatermarkTimeStamp);
    // Once we have read completely from partition, we can advance the watermark associated with the
    // partition
    processContext.updateWatermark(partitionWatermarkTimeStamp);
  }

  @GetInitialRestriction
  @SuppressWarnings("unused")
  public OffsetRange getInitialRestriction(Read readRequest) throws Exception {
    int desiredSplitCount = 1;
    long estimatedSizeBytes = getFileSizeForPartitions(readRequest);
    if (estimatedSizeBytes > 0) {
      desiredSplitCount = (int) Math.ceil((double) estimatedSizeBytes / Integer.MAX_VALUE);
    }
    ReaderContext readerContext = getReaderContext(readRequest, desiredSplitCount);
    // process the splits returned by native API
    // this could be different from 'desiredSplitCount' calculated above
    LOG.info(
        "Splitting: estimated size {}, desired split count {}, actual split count",
        estimatedSizeBytes,
        desiredSplitCount);
    return new OffsetRange(0, readerContext.numSplits());
  }

  @NewTracker
  @SuppressWarnings("unused")
  public OffsetRangeTracker newTracker(OffsetRange range) {
    return range.newTracker();
  }
}
