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
package org.apache.beam.dsls.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.sql.Types;
import java.util.List;
import org.apache.beam.dsls.sql.meta.Column;
import org.apache.beam.dsls.sql.meta.Table;
import org.apache.beam.dsls.sql.meta.provider.MockTableProvider;
import org.apache.beam.dsls.sql.meta.store.InMemoryMetaStore;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for {@link BeamSqlCli}.
 */
public class BeamSqlCliTest {
  private InMemoryMetaStore store;
  private BeamSqlCli cli;

  @Before
  public void setUp() {
    store = new InMemoryMetaStore();
    cli = new BeamSqlCli(store);
  }

  @Test
  public void testCreateTable_complete() throws Exception {
    assertNull(cli.getMetaStore().queryTable("ORDERS"));
    cli.execute(
        "CREATE TABLE ORDERS(\n"
            + "    ID INT PRIMARY KEY COMMENT 'this is the primary key',\n"
            + "    NAME VARCHAR(127) COMMENT 'this is the name'\n"
            + ")\n"
            + "COMMENT 'this is the table orders'\n"
            + "LOCATION 'text://home/admin/orders'\n"
            + "TBLPROPERTIES '{\"format\": \"Excel\"}'"

    );

    Table table = store.queryTable("ORDERS");
    assertNotNull(table);
    List<Column> columns = table.getColumns();
    assertNotNull(columns);
    assertEquals(2, columns.size());
    assertEquals("this is the table orders", table.getComment());
    assertEquals(URI.create("text://home/admin/orders"), table.getLocation());
    assertEquals("text", table.getType());
    assertNotNull(table.getProperties());
    assertEquals("Excel", table.getProperties().getString("format"));

    Column column = columns.get(0);
    assertNotNull(column);
    assertEquals("ID", column.getName());
    assertEquals((Object) Types.INTEGER, column.getType());
    assertTrue(column.isPrimaryKey());
    assertEquals("this is the primary key", column.getComment());

    column = columns.get(1);
    assertNotNull(column);
    assertEquals("NAME", column.getName());
    assertEquals((Object) Types.VARCHAR, column.getType());
    assertFalse(column.isPrimaryKey());
    assertEquals("this is the name", column.getComment());
  }

  @Test
  public void testCreateTable_minimal() throws Exception {
    store.registerProvider(new MockTableProvider());
    cli.defaultTableType("mock");
    cli.execute(
        "CREATE TABLE ORDERS "
            + "("
            + "    ID INT,"
            + "    NAME VARCHAR(127)"
            + ") "
    );
    Table table = store.queryTable("ORDERS");
    assertNotNull(table);
    List<Column> columns = table.getColumns();
    assertNotNull(columns);
    assertEquals(2, columns.size());
    assertEquals("mock", table.getType());
    assertNull(table.getComment());
    assertNull(table.getLocation());
    assertNotNull(table.getProperties());
    assertTrue(table.getProperties().isEmpty());

    Column column = columns.get(0);
    assertNotNull(column);
    assertEquals("ID", column.getName());
    assertEquals((Object) Types.INTEGER, column.getType());
    assertFalse(column.isPrimaryKey());
    assertNull(column.getComment());

    column = columns.get(1);
    assertNotNull(column);
    assertEquals("NAME", column.getName());
    assertEquals((Object) Types.VARCHAR, column.getType());
    assertFalse(column.isPrimaryKey());
    assertNull(column.getComment());
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateTable_tableTypeNotSpecified() throws Exception {
    assertNull(cli.getMetaStore().queryTable("ORDERS"));
    cli.execute(
        "CREATE TABLE ORDERS "
            + "("
            + "    ID INT,"
            + "    NAME VARCHAR(127)"
            + ") "
    );
    store.queryTable("ORDERS");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateTable_duplicateTable() throws Exception {
    cli.execute(
        "CREATE TABLE ORDERS "
            + "(ID INT) "
            + "LOCATION 'text://home/admin/orders'"
    );
    cli.execute(
        "CREATE TABLE ORDERS "
            + "(ID INT) "
            + "LOCATION 'text://home/admin/orders'"
    );
  }
}
