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
import org.junit.Assert;
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

    Assert.assertEquals(6L, sum0);
    Assert.assertEquals(12L, sum1);
  }

  @Test
  public void testInsertSql() {
    List<TableSchema.Column> columns =
        Arrays.asList(
            TableSchema.Column.of("f0", ColumnType.INT64),
            TableSchema.Column.of("f1", ColumnType.INT64));

    String expected = "INSERT INTO \"test_table\" (\"f0\", \"f1\")";

    ClickHouseIO.WriteFn.insertSql(TableSchema.of(columns), "test_table");
  }
}
