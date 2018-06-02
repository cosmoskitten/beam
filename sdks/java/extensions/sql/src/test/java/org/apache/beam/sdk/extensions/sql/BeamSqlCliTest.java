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
package org.apache.beam.sdk.extensions.sql;

import static org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.BOOLEAN;
import static org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.INTEGER;
import static org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.VARCHAR;
import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.calcite.tools.ValidationException;
import org.junit.Test;

/** UnitTest for {@link BeamSqlCli}. */
public class BeamSqlCliTest {
  @Test
  public void testExecute_createTextTable() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age') \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);
    assertEquals(
        Stream.of(
                Field.of("id", INTEGER).withDescription("id").withNullable(true),
                Field.of("name", VARCHAR).withDescription("name").withNullable(true),
                Field.of("age", INTEGER).withDescription("age").withNullable(true))
            .collect(toSchema()),
        table.getSchema());
  }

  @Test
  public void testExecute_createTableWithPrefixArrayField() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age', \n"
            + "tags ARRAY<VARCHAR>, \n"
            + "matrix ARRAY<ARRAY<INTEGER>> \n"
            + ") \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);
    assertEquals(
        Stream.of(
                Field.of("id", INTEGER).withDescription("id").withNullable(true),
                Field.of("name", VARCHAR).withDescription("name").withNullable(true),
                Field.of("age", INTEGER).withDescription("age").withNullable(true),
                Field.of("tags", Schema.FieldType.array(VARCHAR)).withNullable(true),
                Field.of("matrix", Schema.FieldType.array(Schema.FieldType.array(INTEGER)))
                    .withNullable(true))
            .collect(toSchema()),
        table.getSchema());
  }

  @Test
  public void testExecute_createTableWithPrefixMapField() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age', \n"
            + "tags MAP<VARCHAR, VARCHAR>, \n"
            + "nestedMap MAP<INTEGER, MAP<VARCHAR, INTEGER>> \n"
            + ") \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);
    assertEquals(
        Stream.of(
                Field.of("id", INTEGER).withDescription("id").withNullable(true),
                Field.of("name", VARCHAR).withDescription("name").withNullable(true),
                Field.of("age", INTEGER).withDescription("age").withNullable(true),
                Field.of("tags", Schema.FieldType.map(VARCHAR, VARCHAR)).withNullable(true),
                Field.of(
                        "nestedmap",
                        Schema.FieldType.map(INTEGER, Schema.FieldType.map(VARCHAR, INTEGER)))
                    .withNullable(true))
            .collect(toSchema()),
        table.getSchema());
  }

  @Test
  public void testExecute_createTableWithRowField() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age', \n"
            + "address ROW ( \n"
            + "  street VARCHAR, \n"
            + "  country VARCHAR \n"
            + "  ), \n"
            + "addressAngular ROW< \n"
            + "  street VARCHAR, \n"
            + "  country VARCHAR \n"
            + "  >, \n"
            + "isRobot BOOLEAN"
            + ") \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);
    assertEquals(
        Stream.of(
                Field.of("id", INTEGER).withDescription("id").withNullable(true),
                Field.of("name", VARCHAR).withDescription("name").withNullable(true),
                Field.of("age", INTEGER).withDescription("age").withNullable(true),
                Field.of(
                        "address",
                        Schema.FieldType.row(
                            Schema.builder()
                                .addNullableField("street", Schema.FieldType.STRING)
                                .addNullableField("country", Schema.FieldType.STRING)
                                .build()))
                    .withNullable(true),
                Field.of(
                        "addressangular",
                        Schema.FieldType.row(
                            Schema.builder()
                                .addNullableField("street", Schema.FieldType.STRING)
                                .addNullableField("country", Schema.FieldType.STRING)
                                .build()))
                    .withNullable(true),
                Field.of("isrobot", BOOLEAN).withNullable(true))
            .collect(toSchema()),
        table.getSchema());
  }

  @Test
  public void testExecute_dropTable() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age') \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);

    cli.execute("drop table person");
    table = metaStore.getTables().get("person");
    assertNull(table);
  }

  @Test(expected = ValidationException.class)
  public void testExecute_dropTable_assertTableRemovedFromPlanner() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age') \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");
    cli.execute("drop table person");
    cli.explainQuery("select * from person");
  }

  @Test
  public void testExplainQuery() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);

    cli.execute(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age') \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");

    String plan = cli.explainQuery("select * from person");
    assertThat(
        plan,
        equalTo(
            "BeamCalcRel(expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n"
                + "  BeamIOSourceRel(table=[[beam, person]])\n"));
  }
}
