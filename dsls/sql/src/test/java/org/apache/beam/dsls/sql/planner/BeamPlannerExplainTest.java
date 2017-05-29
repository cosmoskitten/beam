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
package org.apache.beam.dsls.sql.planner;

import static org.junit.Assert.assertEquals;

import org.apache.beam.dsls.sql.BeamSqlCli;
import org.junit.Test;

/**
 * Tests to explain queries.
 *
 */
public class BeamPlannerExplainTest extends BasePlanner {
  @Test
  public void selectAll() throws Exception {
    String sql = "SELECT * FROM ORDER_DETAILS";
    String plan = BeamSqlCli.explainQuery(sql);

    String expectedPlan =
        "BeamProjectRel(order_id=[$0], site_id=[$1], price=[$2], order_time=[$3])\n"
        + "  BeamIOSourceRel(table=[[ORDER_DETAILS]])\n";
    assertEquals("explain doesn't match", expectedPlan, plan);
  }

  @Test
  public void selectWithFilter() throws Exception {
    String sql = "SELECT " + " order_id, site_id, price " + "FROM ORDER_DETAILS "
        + "WHERE SITE_ID = 0 and price > 20";
    String plan = BeamSqlCli.explainQuery(sql);

    String expectedPlan = "BeamProjectRel(order_id=[$0], site_id=[$1], price=[$2])\n"
        + "  BeamFilterRel(condition=[AND(=($1, 0), >($2, 20))])\n"
        + "    BeamIOSourceRel(table=[[ORDER_DETAILS]])\n";
    assertEquals("explain doesn't match", expectedPlan, plan);
  }

  @Test
  public void insertSelectFilter() throws Exception {
    String sql = "INSERT INTO SUB_ORDER(order_id, site_id, price) " + "SELECT "
        + " order_id, site_id, price " + "FROM ORDER_DETAILS "
        + "WHERE SITE_ID = 0 and price > 20";
    String plan = BeamSqlCli.explainQuery(sql);

    String expectedPlan =
        "BeamIOSinkRel(table=[[SUB_ORDER]], operation=[INSERT], flattened=[true])\n"
        + "  BeamProjectRel(order_id=[$0], site_id=[$1], price=[$2], order_time=[null])\n"
        + "    BeamProjectRel(order_id=[$0], site_id=[$1], price=[$2])\n"
        + "      BeamFilterRel(condition=[AND(=($1, 0), >($2, 20))])\n"
        + "        BeamIOSourceRel(table=[[ORDER_DETAILS]])\n";
    assertEquals("explain doesn't match", expectedPlan, plan);
  }

  @Test
  public void join() throws Exception {
    String sql = "SELECT * FROM ORDER_DETAILS"
        + " JOIN "
        + "SUB_ORDER_RAM "
        + "on ORDER_DETAILS.order_id = SUB_ORDER_RAM.order_id";
    String plan = BeamSqlCli.explainQuery(sql);
    assertEquals("BeamProjectRel(order_id=[$0], site_id=[$1], price=[$2], "
        + "order_time=[$3], order_id0=[$4], site_id0=[$5], price0=[$6], order_time0=[$7])\n"
        + "  BeamJoinRel(condition=[=($0, $4)], joinType=[inner])\n"
        + "    BeamIOSourceRel(table=[[ORDER_DETAILS]])\n"
        + "    BeamIOSourceRel(table=[[SUB_ORDER_RAM]])\n", plan);
  }
}
