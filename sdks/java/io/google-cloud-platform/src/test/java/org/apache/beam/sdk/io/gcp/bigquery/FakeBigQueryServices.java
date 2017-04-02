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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.fromJsonString;
import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.options.BigQueryOptions;


/**
 * A fake implementation of BigQuery's query service..
 */
class FakeBigQueryServices implements BigQueryServices {
  private JobService jobService;
  private FakeDatasetService datasetService;

  public FakeBigQueryServices withJobService(JobService jobService) {
    this.jobService = jobService;
    return this;
  }

  public FakeBigQueryServices withDatasetService(FakeDatasetService datasetService) {
    this.datasetService = datasetService;
    return this;
  }

  @Override
  public JobService getJobService(BigQueryOptions bqOptions) {
    return jobService;
  }

  @Override
  public DatasetService getDatasetService(BigQueryOptions bqOptions) {
    return datasetService;
  }

  @Override
  public BigQueryJsonReader getReaderFromTable(BigQueryOptions bqOptions, TableReference tableRef) {
    try {
      List<TableRow> rows = datasetService.getAllRows(
          tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId());
      return new FakeBigQueryReader(rows);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public BigQueryJsonReader getReaderFromQuery(
      BigQueryOptions bqOptions, String projectId, JobConfigurationQuery queryConfig) {
    List<TableRow> rows = rowsFromEncodedQuery(queryConfig.getQuery());
    return new FakeBigQueryReader(rows);
  }

  static List<TableRow> rowsFromEncodedQuery(String query) {
    return Lists.newArrayList(BigQueryHelpers.fromJsonString(query, TableRow[].class));
  }

  static String encodeQuery(List<TableRow> rows) {
    return BigQueryHelpers.toJsonString(Iterables.toArray(rows, TableRow.class));
  }

  private static class FakeBigQueryReader implements BigQueryJsonReader {
    private static final int UNSTARTED = -1;
    private static final int CLOSED = Integer.MAX_VALUE;

    private List<String> jsonTableRowReturns;
    private int currIndex;

    FakeBigQueryReader(List<TableRow> tableRowReturns) {
      this.jsonTableRowReturns = Lists.newArrayListWithExpectedSize(tableRowReturns.size());
      for (TableRow tableRow : tableRowReturns) {
        jsonTableRowReturns.add(BigQueryHelpers.toJsonString(tableRow));
      }
      this.currIndex = UNSTARTED;
    }

    @Override
    public boolean start() throws IOException {
      assertEquals(UNSTARTED, currIndex);
      currIndex = 0;
      return currIndex < jsonTableRowReturns.size();
    }

    @Override
    public boolean advance() throws IOException {
      return ++currIndex < jsonTableRowReturns.size();
    }

    @Override
    public TableRow getCurrent() throws NoSuchElementException {
      if (currIndex >= jsonTableRowReturns.size()) {
        throw new NoSuchElementException();
      }
      return fromJsonString(jsonTableRowReturns.get(currIndex), TableRow.class);
    }

    @Override
    public void close() throws IOException {
      currIndex = CLOSED;
    }
  }
}
