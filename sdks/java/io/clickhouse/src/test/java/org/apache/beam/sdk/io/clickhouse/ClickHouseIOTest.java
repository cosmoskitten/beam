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
package org.apache.beam.sdk.io.clickhouse;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.clickhouse.TableSchema.ColumnType;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.ClickHouseContainer;

/** Tests for {@link ClickHouseIO}. */
@RunWith(JUnit4.class)
public class ClickHouseIOTest {

  @ClassRule public static ClickHouseContainer clickhouse = new ClickHouseContainer();
  @Rule public TestPipeline pipeline = TestPipeline.create();

  public void executeSql(String sql) throws SQLException {
    try (Connection connection = clickhouse.createConnection("");
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  public ResultSet executeQuery(String sql) throws SQLException {
    try (Connection connection = clickhouse.createConnection("");
        Statement statement = connection.createStatement(); ) {
      return statement.executeQuery(sql);
    }
  }

  public long executeQueryAsLong(String sql) throws SQLException {
    ResultSet rs = executeQuery(sql);
    rs.next();
    return rs.getLong(1);
  }

  @Test
  public void testInt64() throws Exception {
    Schema schema =
        Schema.of(
            Schema.Field.of("f0", Schema.FieldType.INT64),
            Schema.Field.of("f1", Schema.FieldType.INT64));
    Row row1 = Row.withSchema(schema).addValue(1L).addValue(2L).build();
    Row row2 = Row.withSchema(schema).addValue(2L).addValue(4L).build();
    Row row3 = Row.withSchema(schema).addValue(3L).addValue(6L).build();

    executeSql("CREATE TABLE test_int64 (f0 Int64, f1 Int64) ENGINE=Log");

    pipeline
        .apply(Create.of(row1, row2, row3).withRowSchema(schema))
        .apply(
            ClickHouseIO.Write.builder()
                .table("test_int64")
                .jdbcUrl(clickhouse.getJdbcUrl())
                .build());

    pipeline.run().waitUntilFinish();

    long sum0 = executeQueryAsLong("SELECT SUM(f0) FROM test_int64");
    long sum1 = executeQueryAsLong("SELECT SUM(f1) FROM test_int64");

    assertEquals(6L, sum0);
    assertEquals(12L, sum1);
  }

  @Test
  public void testPrimitiveTypes() throws Exception {
    Schema schema =
        Schema.of(
            Schema.Field.of("f0", Schema.FieldType.DATETIME),
            Schema.Field.of("f1", Schema.FieldType.DATETIME),
            Schema.Field.of("f2", Schema.FieldType.FLOAT),
            Schema.Field.of("f3", Schema.FieldType.DOUBLE),
            Schema.Field.of("f4", Schema.FieldType.BYTE),
            Schema.Field.of("f5", Schema.FieldType.INT16),
            Schema.Field.of("f6", Schema.FieldType.INT32),
            Schema.Field.of("f7", Schema.FieldType.INT64),
            Schema.Field.of("f8", Schema.FieldType.STRING),
            Schema.Field.of("f9", Schema.FieldType.INT16),
            Schema.Field.of("f10", Schema.FieldType.INT32),
            Schema.Field.of("f11", Schema.FieldType.INT64),
            Schema.Field.of("f12", Schema.FieldType.INT64));
    Row row1 =
        Row.withSchema(schema)
            .addValue(new DateTime(2030, 10, 1, 0, 0, 0, DateTimeZone.UTC))
            .addValue(new DateTime(2030, 10, 9, 8, 7, 6, DateTimeZone.UTC))
            .addValue(2.2f)
            .addValue(3.3)
            .addValue((byte) 4)
            .addValue((short) 5)
            .addValue(6)
            .addValue(7L)
            .addValue("eight")
            .addValue((short) 9)
            .addValue(10)
            .addValue(11L)
            .addValue(12L)
            .build();

    executeSql(
        "CREATE TABLE test_primitive_types ("
            + "f0  Date,"
            + "f1  DateTime,"
            + "f2  Float32,"
            + "f3  Float64,"
            + "f4  Int8,"
            + "f5  Int16,"
            + "f6  Int32,"
            + "f7  Int64,"
            + "f8  String,"
            + "f9  UInt8,"
            + "f10 UInt16,"
            + "f11 UInt32,"
            + "f12 UInt64"
            + ") ENGINE=Log");

    pipeline
        .apply(Create.of(row1).withRowSchema(schema))
        .apply(
            ClickHouseIO.Write.builder()
                .table("test_primitive_types")
                .jdbcUrl(clickhouse.getJdbcUrl())
                .build());

    pipeline.run().waitUntilFinish();

    try (ResultSet rs = executeQuery("SELECT * FROM test_primitive_types")) {
      rs.next();

      assertEquals("2030-10-01", rs.getString("f0"));
      assertEquals("2030-10-09 08:07:06", rs.getString("f1"));
      assertEquals("2.2", rs.getString("f2"));
      assertEquals("3.3", rs.getString("f3"));
      assertEquals("4", rs.getString("f4"));
      assertEquals("5", rs.getString("f5"));
      assertEquals("6", rs.getString("f6"));
      assertEquals("7", rs.getString("f7"));
      assertEquals("eight", rs.getString("f8"));
      assertEquals("9", rs.getString("f9"));
      assertEquals("10", rs.getString("f10"));
      assertEquals("11", rs.getString("f11"));
      assertEquals("12", rs.getString("f12"));
    }
  }

  @Test
  public void testInsertSql() {
    List<TableSchema.Column> columns =
        Arrays.asList(
            TableSchema.Column.of("f0", ColumnType.INT64),
            TableSchema.Column.of("f1", ColumnType.INT64));

    String expected = "INSERT INTO \"test_table\" (\"f0\", \"f1\")";

    assertEquals(expected, ClickHouseIO.WriteFn.insertSql(TableSchema.of(columns), "test_table"));
  }
}
