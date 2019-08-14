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
package org.apache.beam.sdk.extensions.zetasketch;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.gcp.testing.BigqueryMatcher;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryHllSketchCompatibilityIT {

  private static final String DATASET_NAME = "zetasketch_compatibility_test";

  // Table for testReadSketchFromBigQuery()
  // Schema: only one STRING field named "data".
  // Content: prepopulated with 4 rows: "Apple", "Orange", "Banana", "Orange"
  private static final String DATA_TABLE_NAME = "hll_data";
  private static final String DATA_FIELD_NAME = "data";
  private static final String QUERY_RESULT_FIELD_NAME = "sketch";
  private static final Long EXPECTED_COUNT = 3L;

  // Table for testWriteSketchToBigQuery()
  // Schema: only one BYTES field named "sketch".
  // Content: will be overridden by the sketch computed by the test pipeline each time the test runs
  private static final String SKETCH_TABLE_NAME = "hll_sketch";
  private static final String SKETCH_FIELD_NAME = "sketch";
  private static final List<String> TEST_DATA =
      Arrays.asList("Apple", "Orange", "Banana", "Orange");
  // SHA-1 hash of string "[3]", the string representation of a row that has only one field 3 in it
  private static final String EXPECTED_CHECKSUM = "f1e31df9806ce94c5bdbbfff9608324930f4d3f1";

  /**
   * Test that HLL++ sketch computed in BigQuery can be processed by Beam. Hll sketch is computed by
   * {@code HLL_COUNT.INIT} in BigQuery and read into Beam; the test verifies that we can run {@link
   * HllCount.MergePartial} and {@link HllCount.Extract} on the sketch in Beam to get the correct
   * estimated count.
   */
  @Test
  public void testReadSketchFromBigQuery() {
    String tableSpec = String.format("%s.%s", DATASET_NAME, DATA_TABLE_NAME);
    String query =
        String.format(
            "SELECT HLL_COUNT.INIT(%s) AS %s FROM %s",
            DATA_FIELD_NAME, QUERY_RESULT_FIELD_NAME, tableSpec);
    SerializableFunction<SchemaAndRecord, byte[]> parseQueryResultToByteArray =
        (SchemaAndRecord schemaAndRecord) ->
            // BigQuery BYTES type corresponds to Java java.nio.ByteBuffer type
            ((ByteBuffer) schemaAndRecord.getRecord().get(QUERY_RESULT_FIELD_NAME)).array();

    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);

    Pipeline p = Pipeline.create(options);
    PCollection<Long> result =
        p.apply(
                BigQueryIO.read(parseQueryResultToByteArray)
                    .fromQuery(query)
                    .usingStandardSql()
                    .withMethod(Method.DIRECT_READ)
                    .withCoder(ByteArrayCoder.of()))
            .apply(HllCount.MergePartial.globally()) // no-op, only for testing MergePartial
            .apply(HllCount.Extract.globally());
    PAssert.thatSingleton(result).isEqualTo(EXPECTED_COUNT);
    p.run().waitUntilFinish();
  }

  /**
   * Test that HLL++ sketch computed in Beam can be processed by BigQuery. Hll sketch is computed by
   * {@link HllCount.Init} in Beam and written to BigQuery; the test verifies that we can run {@code
   * HLL_COUNT.EXTRACT()} on the sketch in BigQuery to get the correct estimated count.
   */
  @Test
  public void testWriteSketchToBigQuery() {
    String tableSpec = String.format("%s.%s", DATASET_NAME, SKETCH_TABLE_NAME);
    String query =
        String.format("SELECT HLL_COUNT.EXTRACT(%s) FROM %s", SKETCH_FIELD_NAME, tableSpec);
    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                Collections.singletonList(
                    new TableFieldSchema().setName(SKETCH_FIELD_NAME).setType("BYTES")));

    TestPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(TestPipelineOptions.class);
    // After the pipeline finishes, BigqueryMatcher will send a query to retrieve the estimated
    // count and verifies its correctness using checksum.
    options.setOnSuccessMatcher(
        BigqueryMatcher.createUsingStandardSql(
            options.as(ApplicationNameOptions.class).getAppName(),
            options.as(GcpOptions.class).getProject(),
            query,
            EXPECTED_CHECKSUM));

    Pipeline p = Pipeline.create(options);
    p.apply(Create.of(TEST_DATA))
        .apply(HllCount.Init.forStrings().globally())
        .apply(
            BigQueryIO.<byte[]>write()
                .to(tableSpec)
                .withSchema(tableSchema)
                .withFormatFunction(sketch -> new TableRow().set(SKETCH_FIELD_NAME, sketch))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
    p.run().waitUntilFinish();
  }
}
