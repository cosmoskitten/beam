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
package org.apache.beam.sdk.bigqueryioperftests;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.synthetic.SyntheticBoundedSource;
import org.apache.beam.sdk.io.synthetic.SyntheticOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A performance test of {@link org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO}.
 *
 * <p>Usage:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/gcp/bigquery -DintegrationTestPipelineOptions='[
 *  "--bigQueryTestDataset=source-dataset",
 *  "--bigQueryTestTable=source-table",
 *  "--bigQueryDataset=metrics-dataset",
 *  "--bigQueryTable=metrics-table",
 *  "--sourceOptions={"numRecords":"1000", "keySize":1, valueSize:"1024"}
 *  }"]'
 *  --tests org.apache.beam.sdk.io.gcp.bigQuery.BigQueryIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 */
@RunWith(JUnit4.class)
public class BigQueryIOIT {
  private static final String NAMESPACE = BigQueryIOIT.class.getName();
  private static String bigQueryMetricsTable;
  private static String bigQueryMetricsDataset;
  private static String bigQueryTestDataset;
  private static String bigQueryTestTable;
  private static SyntheticSourceOptions sourceOptions;
  private static String tableQualifier;
  private static String tempRoot;
  private static Boolean useStreamingWrites;
  private static BigQueryPerfTestOptions options;
  private static final String TEST_ID = UUID.randomUUID().toString();
  private static final String TEST_TIMESTAMP = Timestamp.now().toString();
  private static final String READ_TIME_METRIC_NAME = "read_time";
  private static final String WRITE_TIME_METRIC_NAME = "write_time";

  /** Options for this io performance test. */
  public interface BigQueryPerfTestOptions extends IOTestPipelineOptions {
    @Description("Synthetic source options")
    @Validation.Required
    String getSourceOptions();

    void setSourceOptions(String value);

    @Description("BQ dataset for the test data")
    String getBigQueryTestDataset();

    void setBigQueryTestDataset(String dataset);

    @Description("BQ table for test data")
    String getBigQueryTestTable();

    void setBigQueryTestTable(String table);

    @Description("Should test use streaming writes or batch loads to BQ")
    Boolean getUseStreamingWrites();

    void setUseStreamingWrites(Boolean value);
  }

  @BeforeClass
  public static void setup() throws IOException {
    options = IOITHelper.readIOTestPipelineOptions(BigQueryPerfTestOptions.class);
    tempRoot = options.getTempRoot();
    sourceOptions =
        SyntheticOptions.fromJsonString(options.getSourceOptions(), SyntheticSourceOptions.class);
    bigQueryMetricsDataset = options.getBigQueryDataset();
    bigQueryMetricsTable = options.getBigQueryTable();
    bigQueryTestDataset = options.getBigQueryTestDataset();
    bigQueryTestTable = options.getBigQueryTestTable();
    useStreamingWrites = options.getUseStreamingWrites();
    BigQueryOptions bigQueryOptions = BigQueryOptions.newBuilder().build();
    tableQualifier =
        String.format(
            "%s:%s.%s", bigQueryOptions.getProjectId(), bigQueryTestDataset, bigQueryTestTable);
  }

  @AfterClass
  public static void tearDown() {
    BigQueryOptions options = BigQueryOptions.newBuilder().build();
    BigQuery client = options.getService();
    TableId tableId = TableId.of(options.getProjectId(), bigQueryTestDataset, bigQueryTestTable);
    client.delete(tableId);
  }

  private void testWrite() {
    Pipeline pipeline = Pipeline.create(options);

    BigQueryIO.Write.Method method =
        useStreamingWrites
            ? BigQueryIO.Write.Method.STREAMING_INSERTS
            : BigQueryIO.Write.Method.FILE_LOADS;
    pipeline
        .apply("Read from source", Read.from(new SyntheticBoundedSource(sourceOptions)))
        .apply("Map records", ParDo.of(new MapKVToV()))
        .apply("Gather time", ParDo.of(new TimeMonitor<>(NAMESPACE, WRITE_TIME_METRIC_NAME)))
        .apply(
            "Write to BQ",
            BigQueryIO.<byte[]>write()
                .to(tableQualifier)
                .withFormatFunction(
                    input -> {
                      TableRow tableRow = new TableRow();
                      tableRow.set("data", input);
                      return tableRow;
                    })
                .withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of(tempRoot))
                .withMethod(method)
                .withSchema(
                    new TableSchema()
                        .setFields(
                            Collections.singletonList(
                                new TableFieldSchema().setName("data").setType("BYTES")))));

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();
    extractAndPublishTime(pipelineResult, WRITE_TIME_METRIC_NAME);
  }

  private void extractAndPublishTime(PipelineResult pipelineResult, String writeTimeMetricName) {
    NamedTestResult metricResult =
        getMetricSupplier(writeTimeMetricName).apply(new MetricsReader(pipelineResult, NAMESPACE));
    IOITMetrics.publish(
        TEST_ID,
        TEST_TIMESTAMP,
        bigQueryMetricsDataset,
        bigQueryMetricsTable,
        Collections.singletonList(metricResult));
  }

  @Test
  public void testWriteThenRead() {
    testWrite();
    testRead();
  }

  private void testRead() {
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply("Read from BQ", BigQueryIO.readTableRows().from(tableQualifier))
        .apply("Gather time", ParDo.of(new TimeMonitor<>(NAMESPACE, READ_TIME_METRIC_NAME)));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    extractAndPublishTime(result, READ_TIME_METRIC_NAME);
  }

  private static Function<MetricsReader, NamedTestResult> getMetricSupplier(String metricName) {
    return reader -> {
      long startTime = reader.getStartTimeMetric(metricName);
      long endTime = reader.getEndTimeMetric(metricName);
      return NamedTestResult.create(
          TEST_ID, TEST_TIMESTAMP, metricName, (endTime - startTime) / 1e3);
    };
  }

  private static class MapKVToV extends DoFn<KV<byte[], byte[]>, byte[]> {
    @ProcessElement
    public void process(ProcessContext context) {
      context.output(context.element().getValue());
    }
  }
}
