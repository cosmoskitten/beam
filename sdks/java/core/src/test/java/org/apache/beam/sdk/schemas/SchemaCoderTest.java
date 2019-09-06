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
package org.apache.beam.sdk.schemas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link Schema}. */
public class SchemaCoderTest {
  private static final Schema INT32_SCHEMA =
      withId(Schema.builder().addInt32Field("a").addInt32Field("b").build());
  private static final Schema FOO_SCHEMA = withId(Schema.builder().addStringField("foo").build());

  private static Schema withId(Schema schema) {
    if (schema.getUUID() == null) {
      schema.setUUID(UUID.randomUUID());
    }
    return schema;
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void equals_sameCoder_returnsTrue() {
    SchemaCoder coder = SchemaCoder.of(FOO_SCHEMA);
    assertEquals(coder, coder);
  }

  @Test
  public void equals_sameSchemaBothRow_returnsTrue() {
    assertEquals(SchemaCoder.of(FOO_SCHEMA), SchemaCoder.of(FOO_SCHEMA));
  }

  @Test
  public void equals_sameSchemaDifferentType_returnsFalse() {
    SchemaCoder coder = SchemaCoder.of(FOO_SCHEMA);
    SchemaCoder coderWithType =
        SchemaCoder.of(FOO_SCHEMA, TypeDescriptor.of(FooRow.class), FooRow::toRow, FooRow::fromRow);
    assertNotEquals(coder, coderWithType);
  }

  @Test
  public void equals_sameSchemaSameType_returnsTrue() {
    SchemaCoder coderA =
        SchemaCoder.of(FOO_SCHEMA, TypeDescriptor.of(FooRow.class), FooRow::toRow, FooRow::fromRow);
    SchemaCoder coderB =
        SchemaCoder.of(FOO_SCHEMA, TypeDescriptor.of(FooRow.class), FooRow::toRow, FooRow::fromRow);
    assertEquals(coderA, coderB);
  }

  @Test
  public void equals_sameSchemaSameGenericType_returnsTrue() {
    SchemaCoder coderA =
        SchemaCoder.of(
            INT32_SCHEMA,
            TypeDescriptors.lists(TypeDescriptors.integers()),
            (List<Integer> l) ->
                Row.withSchema(INT32_SCHEMA).addValue(l.get(0)).addValue(l.get(1)).build(),
            (Row row) -> ImmutableList.of(row.getInt32(0), row.getInt32(1)));
    SchemaCoder coderB =
        SchemaCoder.of(
            INT32_SCHEMA,
            TypeDescriptors.lists(TypeDescriptors.integers()),
            (List<Integer> l) ->
                Row.withSchema(INT32_SCHEMA).addValue(l.get(0)).addValue(l.get(1)).build(),
            (Row row) -> ImmutableList.of(row.getInt32(0), row.getInt32(1)));

    assertEquals(coderA, coderB);
  }

  @Test
  public void equals_sameSchemaDifferentGenericType_returnsFalse() {
    SchemaCoder int_list_coder =
        SchemaCoder.of(
            INT32_SCHEMA,
            TypeDescriptors.lists(TypeDescriptors.integers()),
            (List<Integer> l) ->
                Row.withSchema(INT32_SCHEMA).addValue(l.get(0)).addValue(l.get(1)).build(),
            (Row row) -> ImmutableList.of(row.getInt32(0), row.getInt32(1)));
    SchemaCoder string_list_coder =
        SchemaCoder.of(
            INT32_SCHEMA,
            TypeDescriptors.lists(TypeDescriptors.strings()),
            (List<String> l) ->
                Row.withSchema(INT32_SCHEMA)
                    .addValue(Integer.parseInt(l.get(1)))
                    .addValue(Integer.parseInt(l.get(1)))
                    .build(),
            (Row row) -> ImmutableList.of(row.getInt32(0).toString(), row.getInt32(1).toString()));

    assertNotEquals(int_list_coder, string_list_coder);
  }

  private static class FooRow {
    public final String foo;

    public FooRow(String foo) {
      this.foo = foo;
    }

    public static FooRow fromRow(Row row) {
      return new FooRow(row.getValue("foo"));
    }

    public static Row toRow(FooRow fooRow) {
      return Row.withSchema(FOO_SCHEMA).addValue(fooRow.foo).build();
    }
  }
}
