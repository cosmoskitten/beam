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
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;

public class AtomicInsertTest {
  private static ClickHouseContainer clickhouse;
  private static GenericContainer zookeeper;

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

  @BeforeClass
  public static void setup() {
    zookeeper =
        new GenericContainer<>("zookeeper:3.3.6")
            .withExposedPorts(2181)
            .withNetworkAliases("zookeeper");
    zookeeper.start();

    clickhouse =
        (ClickHouseContainer)
            new ClickHouseContainer()
                .withPrivilegedMode(true)
                .withClasspathResourceMapping(
                    "config.d/zookeeper_default.xml",
                    "/etc/clickhouse-server/config.d/zookeeper_default.xml",
                    BindMode.READ_ONLY);

    clickhouse.start();
  }

  @AfterClass
  public static void teardown() {
    zookeeper.close();
    clickhouse.close();
  }

  @Test
  public void testAtomicInsert() throws SQLException {
    Schema schema = Schema.of(Schema.Field.of("f0", Schema.FieldType.INT64));
    int size = 1000000;

    executeSql(
        "CREATE TABLE test_atomic_insert ("
            + "  f0 Int64, "
            + "  f1 Int64 MATERIALIZED CAST(if((rand() % "
            + size
            + ") = 0, '', '1') AS Int64)"
            + ") ENGINE=MergeTree ORDER BY (f0)");

    Iterable<Row> bundle =
        IntStream.range(0, size)
            .mapToObj(x -> Row.withSchema(schema).addValue((long) x).build())
            .collect(Collectors.toList());

    int failed = 0;
    int done = 0;

    pipeline
        // make sure we get one big bundle
        .apply(Create.<Iterable<Row>>of(bundle).withCoder(IterableCoder.of(RowCoder.of(schema))))
        .apply(Flatten.iterables())
        .setRowSchema(schema)
        .apply(
            ClickHouseIO.Write.builder()
                .jdbcUrl(clickhouse.getJdbcUrl())
                .table("test_atomic_insert")
                .properties(
                    ClickHouseIO.properties().maxBlockSize(size).maxInsertBlockSize(size).build())
                .build());

    do {
      try {
        final PipelineResult.State state = pipeline.run().waitUntilFinish();

        if (state == PipelineResult.State.DONE) {
          done++;
        } else if (state == PipelineResult.State.FAILED) {
          failed++;
        } else {
          fail("Unexpected state" + state);
        }
      } catch (Pipeline.PipelineExecutionException e) {
        failed++;
      }

    } while (failed == 0 || done == 0);

    long count = executeQueryAsLong("SELECT COUNT(*) FROM test_atomic_insert");

    assertEquals(done * size, count);
  }
}
