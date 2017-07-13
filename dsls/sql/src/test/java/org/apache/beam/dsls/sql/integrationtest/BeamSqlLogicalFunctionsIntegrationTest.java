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
package org.apache.beam.dsls.sql.integrationtest;

import java.sql.Types;
import org.apache.beam.dsls.sql.BeamSql;
import org.apache.beam.dsls.sql.BeamSqlDslBase;
import org.apache.beam.dsls.sql.TestUtils;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * Integration test for logical functions.
 */
public class BeamSqlLogicalFunctionsIntegrationTest extends BeamSqlDslBase{
  @Test
  public void testStringFunctions() throws Exception {
    String sql = "SELECT "
        + "f_int = 1 AND f_long = 1000 as true1,"
        + "f_int = 1 OR f_long = 2000 as true2,"
        + "NOT f_long = 2000 as true3, "
        + "(NOT f_long = 2000) AND (f_int = 1 OR f_long = 3000) as true4, "
        + "f_int = 2 AND f_long = 1000 as false1,"
        + "f_int = 2 OR f_long = 2000 as false2,"
        + "NOT f_long = 1000 as false3, "
        + "(NOT f_long = 2000) AND (f_int = 2 OR f_long = 3000) as false4 "
        + "FROM PCOLLECTION"
    ;

    PCollection<BeamSqlRow> rows = boundedInput2.apply(BeamSql.simpleQuery(sql));
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.BOOLEAN, "true1",
            Types.BOOLEAN, "true2",
            Types.BOOLEAN, "true3",
            Types.BOOLEAN, "true4",
            Types.BOOLEAN, "false1",
            Types.BOOLEAN, "false2",
            Types.BOOLEAN, "false3",
            Types.BOOLEAN, "false4"
        ).addRows(
            true, true, true, true,
            false, false, false, false
        ).getRows());
    pipeline.run();
  }

}
