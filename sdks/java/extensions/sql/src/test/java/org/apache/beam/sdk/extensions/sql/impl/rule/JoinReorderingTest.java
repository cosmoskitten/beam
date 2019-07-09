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
package org.apache.beam.sdk.extensions.sql.impl.rule;

import java.math.BigInteger;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test ensures that we are reordering joins and get a plan similar to Join(large,Join(small,
 * medium)) instead of Join(small, Join(medium,large).
 */
public class JoinReorderingTest {

  @Test
  public void test_table_sizes() {
    TestTableProvider tableProvider = new TestTableProvider();
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);
    createThreeTables(env, tableProvider);
    Assert.assertEquals(
        BigInteger.ONE,
        tableProvider
            .buildBeamSqlTable(tableProvider.getTable("small_table"))
            .getRowCount(null)
            .getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(3),
        tableProvider
            .buildBeamSqlTable(tableProvider.getTable("medium_table"))
            .getRowCount(null)
            .getRowCount());

    Assert.assertEquals(
        BigInteger.valueOf(100),
        tableProvider
            .buildBeamSqlTable(tableProvider.getTable("large_table"))
            .getRowCount(null)
            .getRowCount());
  }

  @Test
  public void test_correct_reordering() {
    TestTableProvider tableProvider = new TestTableProvider();
    BeamSqlEnv env = BeamSqlEnv.withTableProvider(tableProvider);
    createThreeTables(env, tableProvider);

    BeamRelNode parsedQuery =
        env.parseQuery(
            "select * from large_table "
                + " JOIN medium_table on large_table.medium_key = medium_table.large_key "
                + " JOIN small_table on medium_table.small_key = small_table.medium_key ");
    assertLargeIsTop(parsedQuery);

    parsedQuery =
        env.parseQuery(
            "select * from medium_table "
                + " JOIN large_table on large_table.medium_key = medium_table.large_key "
                + " JOIN small_table on medium_table.small_key = small_table.medium_key ");
    assertLargeIsTop(parsedQuery);

    parsedQuery =
        env.parseQuery(
            "select * from medium_table "
                + " JOIN small_table on medium_table.small_key = small_table.medium_key "
                + " JOIN large_table on large_table.medium_key = medium_table.large_key ");
    assertLargeIsTop(parsedQuery);

    parsedQuery =
        env.parseQuery(
            "select * from small_table "
                + " JOIN medium_table on medium_table.small_key = small_table.medium_key "
                + " JOIN large_table on large_table.medium_key = medium_table.large_key ");
    assertLargeIsTop(parsedQuery);
  }

  private void assertLargeIsTop(BeamRelNode parsedQuery) {
    RelNode firstJoin = parsedQuery;
    while (!(firstJoin instanceof Join)) {
      firstJoin = firstJoin.getInput(0);
    }

    RelNode topRight = ((Join) firstJoin).getRight();
    while (!(topRight instanceof Join) && !(topRight instanceof BeamIOSourceRel)) {
      topRight = topRight.getInput(0);
    }

    if (topRight instanceof BeamIOSourceRel) {
      Assert.assertTrue(topRight.getDescription().contains("large_table"));
    } else {
      RelNode topLeft = ((Join) firstJoin).getLeft();
      while (!(topLeft instanceof BeamIOSourceRel)) {
        topLeft = topLeft.getInput(0);
      }

      Assert.assertTrue(topLeft.getDescription().contains("large_table"));
    }
  }

  private void createThreeTables(BeamSqlEnv env, TestTableProvider tableProvider) {
    env.executeDdl("CREATE EXTERNAL TABLE small_table (id INTEGER, medium_key INTEGER) TYPE text");

    env.executeDdl(
        "CREATE EXTERNAL TABLE medium_table ("
            + "id INTEGER,"
            + "small_key INTEGER,"
            + "large_key INTEGER"
            + ") TYPE text");

    env.executeDdl(
        "CREATE EXTERNAL TABLE large_table ("
            + "id INTEGER,"
            + "medium_key INTEGER"
            + ") TYPE text");

    Row row =
        Row.withSchema(tableProvider.getTable("small_table").getSchema()).addValues(1, 1).build();
    tableProvider.addRows("small_table", row);

    for (int i = 0; i < 3; i++) {
      row =
          Row.withSchema(tableProvider.getTable("medium_table").getSchema())
              .addValues(i, 1, 2)
              .build();
      tableProvider.addRows("medium_table", row);
    }

    for (int i = 0; i < 100; i++) {
      row =
          Row.withSchema(tableProvider.getTable("large_table").getSchema()).addValues(i, 2).build();
      tableProvider.addRows("large_table", row);
    }
  }
}
