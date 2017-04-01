package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.fromJsonString;
import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.options.BigQueryOptions;


/**
 * Created by relax on 3/30/17.
 */
class FakeBigQueryServices implements BigQueryServices {
  private String[] jsonTableRowReturns = new String[0];
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

  public FakeBigQueryServices readerReturns(String... jsonTableRowReturns) {
    this.jsonTableRowReturns = jsonTableRowReturns;
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

  List<TableRow> rowsFromEncodedQuery(String query) {
    return (List<TableRow>) BigQueryHelpers.fromJsonString(query, List.class);
  }

  String encodeQuery(List<TableRow> rows) {
    return BigQueryHelpers.toJsonString(rows);
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
