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
package org.apache.beam.dsls.sql.meta.provider.text;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSONObject;
import java.net.URI;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.dsls.sql.meta.Column;
import org.apache.beam.dsls.sql.meta.Table;
import org.apache.beam.dsls.sql.schema.BeamSqlTable;
import org.apache.commons.csv.CSVFormat;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for {@link TextTableProvider}.
 */
public class TextTableProviderTest {
  private static TextTableProvider provider;

  @BeforeClass
  public static void setUp() {
    provider = new TextTableProvider();
  }

  @Test public void buildTable() throws Exception {
    List<Column> columns = new ArrayList<>();
    columns.add(Column.builder().name("id").type(Types.VARCHAR).primaryKey(false).build());
    columns.add(Column.builder().name("name").type(Types.VARCHAR).primaryKey(false).build());
    columns.add(Column.builder().name("age").type(Types.INTEGER).primaryKey(false).build());

    JSONObject properties = new JSONObject();
    properties.put("format", "Excel");
    Table table = Table.builder()
        .name("orders")
        .type("text")
        .columns(columns)
        .location(URI.create("text:///home/admin/person.txt"))
        .properties(properties)
        .build();

    BeamSqlTable beamSqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(beamSqlTable);
    assertTrue(beamSqlTable instanceof BeamTextCSVTable);
    BeamTextCSVTable textCSVTable = (BeamTextCSVTable) beamSqlTable;
    assertEquals("/home/admin/person.txt", textCSVTable.getFilePattern());
    assertEquals(CSVFormat.EXCEL, textCSVTable.getCsvFormat());
  }

  @Test public void buildTable_defaultCsvFormat() throws Exception {
    List<Column> columns = new ArrayList<>();
    columns.add(Column.builder().name("id").type(Types.VARCHAR).primaryKey(false).build());

    Table table = Table.builder()
        .name("orders")
        .type("text")
        .columns(columns)
        .location(URI.create("text:///home/admin/person.txt"))
        .properties(new JSONObject())
        .build();

    BeamSqlTable beamSqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(beamSqlTable);
    assertTrue(beamSqlTable instanceof BeamTextCSVTable);
    BeamTextCSVTable textCSVTable = (BeamTextCSVTable) beamSqlTable;
    assertEquals(CSVFormat.DEFAULT, textCSVTable.getCsvFormat());
  }
}
