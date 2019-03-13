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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.Storage.Stream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link org.apache.beam.sdk.io.Source} representing reading the results of a query. */
public class BigQueryStorageQuerySource<T> extends BoundedSource<T> {

  /**
   * The maximum number of streams which will be requested when creating a read session, regardless
   * of the desired bundle size.
   */
  private static final int MAX_SPLIT_COUNT = 10_000;

  /**
   * The minimum number of streams which will be requested when creating a read session, regardless
   * of the desired bundle size. Note that the server may still choose to return fewer than this
   * number of streams based on the layout of the table.
   */
  private static final int MIN_SPLIT_COUNT = 10;

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryStorageQuerySource.class);

  public static <T> BigQueryStorageQuerySource<T> create(
      String stepUuid,
      ValueProvider<String> queryProvider,
      Boolean flattenResults,
      Boolean useLegacySql,
      QueryPriority priority,
      @Nullable String location,
      @Nullable String kmsKey,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryStorageQuerySource<>(
        stepUuid,
        queryProvider,
        flattenResults,
        useLegacySql,
        priority,
        location,
        kmsKey,
        parseFn,
        outputCoder,
        bqServices);
  }

  private final String stepUuid;
  private final ValueProvider<String> queryProvider;
  private final Boolean flattenResults;
  private final Boolean useLegacySql;
  private final QueryPriority priority;
  private final String location;
  private final String kmsKey;
  private final SerializableFunction<SchemaAndRecord, T> parseFn;
  private final Coder<T> outputCoder;
  private final BigQueryServices bqServices;

  private transient AtomicReference<BigQueryQueryHelper> queryHelperReference;

  private BigQueryStorageQuerySource(
      String stepUuid,
      ValueProvider<String> queryProvider,
      Boolean flattenResults,
      Boolean useLegacySql,
      QueryPriority priority,
      @Nullable String location,
      @Nullable String kmsKey,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    this.stepUuid = checkNotNull(stepUuid, "stepUuid");
    this.queryProvider = checkNotNull(queryProvider, "queryProvider");
    this.flattenResults = checkNotNull(flattenResults, "flattenResults");
    this.useLegacySql = checkNotNull(useLegacySql, "useLegacySql");
    this.priority = checkNotNull(priority, "priority");
    this.location = location;
    this.kmsKey = kmsKey;
    this.parseFn = checkNotNull(parseFn, "parseFn");
    this.outputCoder = checkNotNull(outputCoder, "outputCoder");
    this.bqServices = checkNotNull(bqServices, "bqServices");
    this.queryHelperReference = new AtomicReference<>();
  }

  private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
    in.defaultReadObject();
    queryHelperReference = new AtomicReference<>();
  }

  @Override
  public Coder<T> getOutputCoder() {
    return outputCoder;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("query", queryProvider).withLabel("Query"));
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    BigQueryQueryHelper queryHelper = getQueryHelper();
    return queryHelper.dryRunQueryIfNeeded(bqOptions).getQuery().getTotalBytesProcessed();
  }

  @Override
  public List<BigQueryStorageStreamSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    BigQueryQueryHelper queryHelper = getQueryHelper();
    TableReference tableReference = queryHelper.executeQuery(bqOptions);
    Table table = bqServices.getDatasetService(bqOptions).getTable(tableReference);
    long tableSizeBytes = (table != null) ? table.getNumBytes() : 0;

    int streamCount = 0;
    if (desiredBundleSizeBytes > 0) {
      streamCount = (int) Math.min(tableSizeBytes / desiredBundleSizeBytes, MAX_SPLIT_COUNT);
    }

    CreateReadSessionRequest request =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/" + bqOptions.getProject())
            .setTableReference(BigQueryHelpers.toTableRefProto(tableReference))
            .setRequestedStreams(Math.max(streamCount, MIN_SPLIT_COUNT))
            .build();

    ReadSession readSession;
    try (StorageClient client = bqServices.getStorageClient(bqOptions)) {
      readSession = client.createReadSession(request);
    }

    if (readSession.getStreamsList().isEmpty()) {
      // The underlying table is empty or has no rows which can be read.
      return ImmutableList.of();
    }

    List<BigQueryStorageStreamSource<T>> sources = Lists.newArrayList();
    for (Stream stream : readSession.getStreamsList()) {
      sources.add(
          BigQueryStorageStreamSource.create(
              readSession, stream, table.getSchema(), parseFn, outputCoder, bqServices));
    }

    return ImmutableList.copyOf(sources);
  }

  @Override
  public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
    throw new UnsupportedOperationException("BigQuery query source must be split before reading");
  }

  private synchronized BigQueryQueryHelper getQueryHelper() {
    if (queryHelperReference.get() == null) {
      BigQueryQueryHelper queryHelper =
          new BigQueryQueryHelper(
              stepUuid,
              queryProvider,
              flattenResults,
              useLegacySql,
              priority,
              location,
              kmsKey,
              bqServices);
      queryHelperReference.compareAndSet(null, queryHelper);
    }
    return queryHelperReference.get();
  }
}
