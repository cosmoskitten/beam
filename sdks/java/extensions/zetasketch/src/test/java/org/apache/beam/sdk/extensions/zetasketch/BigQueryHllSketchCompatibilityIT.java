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

import java.nio.ByteBuffer;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQueryHllSketchCompatibilityIT {

  private static final String DATASET_NAME = "robinyq";
  private static final String READ_TABLE_NAME = "hll_data";
  private static final String READ_TABLE_FIELD_NAME = "strings";
  private static final String READ_QUERY_RESULT_FIELD_NAME = "sketch";
  private static final Long READ_EXCPECTED_RESULT = 3L;

  @Test
  public void testReadSketchFromBigQuery() {
    DataflowPipelineOptions options =
        TestPipeline.testingPipelineOptions().as(DataflowPipelineOptions.class);

    // To be removed
    options.setWorkerHarnessContainerImage("");
    options.setDataflowWorkerJar(
        "/usr/local/google/home/robinyq/beam/runners/google-cloud-dataflow-java/worker/legacy-worker/build/libs/beam-runners-google-cloud-dataflow-java-legacy-worker-2.15.0-SNAPSHOT.jar");

    String sql =
        String.format(
            "SELECT HLL_COUNT.INIT(%s) AS %s FROM %s.%s",
            READ_TABLE_FIELD_NAME, READ_QUERY_RESULT_FIELD_NAME, DATASET_NAME, READ_TABLE_NAME);
    Pipeline p = Pipeline.create(options);
    PCollection<Long> result =
        p.apply(
                BigQueryIO.read(
                        (SchemaAndRecord schemaAndRecord) ->
                            ((ByteBuffer)
                                    schemaAndRecord.getRecord().get(READ_QUERY_RESULT_FIELD_NAME))
                                .array())
                    .fromQuery(sql)
                    .usingStandardSql()
                    .withMethod(Method.DIRECT_READ)
                    .withCoder(ByteArrayCoder.of()))
            .apply(HllCount.MergePartial.globally())
            .apply(HllCount.Extract.globally());
    PAssert.thatSingleton(result).isEqualTo(READ_EXCPECTED_RESULT);
    p.run().waitUntilFinish();
  }

  @Test
  public void testWriteSketchToBigQuery() {
    /*
    Pipeline p = Pipeline.create(options);
    List<String> strs = Arrays.asList("Apple", "Orange", "Banana", "Orange");
    p.apply(Create.of(strs))
        .apply(
            BigQueryIO.<String>write()
                .to("robinyq.hll_data")
                .withSchema(
                    new TableSchema()
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("strings").setType("STRING"))))
                .withFormatFunction(str -> new TableRow().set("strings", str))
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
    p.run().waitUntilFinish();
    */
  }
}
