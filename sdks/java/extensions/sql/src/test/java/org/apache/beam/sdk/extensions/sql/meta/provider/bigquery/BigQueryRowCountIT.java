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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.gcp.bigquery.TestBigQuery;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests form writing to BigQuery with Beam SQL. */
@RunWith(JUnit4.class)
public class BigQueryRowCountIT {
  private static final Schema SOURCE_SCHEMA =
      Schema.builder().addNullableField("id", INT64).addNullableField("name", STRING).build();

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient TestBigQuery bigQuery = TestBigQuery.create(SOURCE_SCHEMA);

  @Test
  public void testEmptyTable() {
    BigQueryTableProvider provider = new BigQueryTableProvider();
    Table table = getTable("testTable", bigQuery.tableSpec());
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);
    Double size = sqlTable.getRowCount(TestPipeline.testingPipelineOptions());
    assertNotNull(size);
    assertEquals(0, size, 0.1);
  }

  @Test
  public void testNonEmptyTable() {
    BigQueryTableProvider provider = new BigQueryTableProvider();
    Table table = getTable("testTable", bigQuery.tableSpec());
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(provider);

    String createTableStatement =
        "CREATE EXTERNAL TABLE TEST( \n"
            + "   id INTEGER, \n"
            + "   name VARCHAR \n"
            + ") \n"
            + "TYPE 'bigquery' \n"
            + "LOCATION '"
            + bigQuery.tableSpec()
            + "'";
    sqlEnv.executeDdl(createTableStatement);

    String insertStatement = "INSERT INTO TEST VALUES (" + "1, " + "'some name'" + ")";

    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);
    Double size0 = sqlTable.getRowCount(TestPipeline.testingPipelineOptions());

    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(insertStatement));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(5));

    sqlTable = provider.buildBeamSqlTable(table);
    Double size1 = sqlTable.getRowCount(TestPipeline.testingPipelineOptions());

    assertNotNull(size0);
    assertNotNull(size1);
    assertEquals(1, size1 - size0, 0.1);
  }

  @Test
  public void testFakeTable() {
    BigQueryTableProvider provider = new BigQueryTableProvider();
    Table table = getTable("fakeTable", "project:dataset.table");

    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);
    Double size = sqlTable.getRowCount(TestPipeline.testingPipelineOptions());
    assertNull(size);
  }

  private static Table getTable(String name, String location) {
    return Table.builder()
        .name(name)
        .comment(name + " table")
        .location(location)
        .schema(
            Stream.of(Schema.Field.nullable("id", INT64), Schema.Field.nullable("name", STRING))
                .collect(toSchema()))
        .type("bigquery")
        .build();
  }
}
