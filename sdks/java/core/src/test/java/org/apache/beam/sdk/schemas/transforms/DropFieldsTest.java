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
package org.apache.beam.sdk.schemas.transforms;

import static org.junit.Assert.assertEquals;

import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Tests for {@link DropFields}.
 *
 */
public class DropFieldsTest {
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  private static final Schema SIMPLE_SCHEMA = Schema.builder()
      .addInt32Field("field1")
      .addStringField("field2")
      .build();
  private static Row simpleRow(int field1, String field2) {
    return Row.withSchema(SIMPLE_SCHEMA).addValues(field1, field2).build();
  }

  private static final Schema NESTED_SCHEMA = Schema.builder()
      .addRowField("nested", SIMPLE_SCHEMA)
      .build();

  private static Row nestedRow(Row nested) {
    return Row.withSchema(NESTED_SCHEMA).addValue(nested).build();
  }

  private static final Schema NESTED_ARRAY_SCHEMA = Schema.builder()
      .addArrayField("array", FieldType.row(SIMPLE_SCHEMA))
      .build();

  private static Row nestedArray(Row... elements) {
    return Row.withSchema(NESTED_ARRAY_SCHEMA).addArray((Object[]) elements).build();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDropTopLevelField() {
    Schema expectedSchema = Schema.builder().addStringField("field2").build();

    PCollection<Row> result = pipeline.apply(Create.of(
        simpleRow(1, "one"), simpleRow(2, "two"), simpleRow(3, "three"))
        .withRowSchema(SIMPLE_SCHEMA))
        .apply(DropFields.fieldNames("field1"));
    assertEquals(expectedSchema, result.getSchema());

    List<Row> expectedRows = Lists.newArrayList(
        Row.withSchema(expectedSchema).addValue("one").build(),
        Row.withSchema(expectedSchema).addValue("two").build(),
        Row.withSchema(expectedSchema).addValue("three").build());
    PAssert.that(result).containsInAnyOrder(expectedRows);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDropNestedField() {
    Schema expectedInnerSchema = Schema.builder().addStringField("field2").build();
    Schema expectedSchema = Schema.builder().addRowField("nested",
        expectedInnerSchema).build();

    PCollection<Row> result = pipeline.apply(Create.of(
        nestedRow(simpleRow(1, "one")),
        nestedRow(simpleRow(2, "two")),
        nestedRow(simpleRow(3, "three")))
        .withRowSchema(NESTED_SCHEMA))
        .apply(DropFields.fieldNames("nested.field1"));
    assertEquals(expectedSchema, result.getSchema());

    List<Row> expectedRows = Lists.newArrayList(
        nestedRow(Row.withSchema(expectedInnerSchema).addValue("one").build()),
        nestedRow(Row.withSchema(expectedInnerSchema).addValue("two").build()),
        nestedRow(Row.withSchema(expectedInnerSchema).addValue("three").build()));
    PAssert.that(result).containsInAnyOrder(expectedRows);
    pipeline.run();
  }


  @Test
  @Category(NeedsRunner.class)
  public void testDropNestedArrayField() {
    Schema expectedInnerSchema = Schema.builder().addStringField("field2").build();
    Schema expectedSchema = Schema.builder().addArrayField("array",
        FieldType.row(expectedInnerSchema)).build();

    PCollection<Row> result = pipeline.apply(Create.of(
        nestedArray(simpleRow(1, "one1"), simpleRow(1, "one2")),
        nestedArray(simpleRow(2, "two1"), simpleRow(2, "two2")),
        nestedArray(simpleRow(3, "three1"), simpleRow(3, "three2")))
        .withRowSchema(NESTED_ARRAY_SCHEMA))
        .apply(DropFields.fieldNames("array[].field1"));
    assertEquals(expectedSchema, result.getSchema());

    List<Row> expectedRows = Lists.newArrayList(
        nestedArray(
            Row.withSchema(expectedInnerSchema).addValue("one1").build(),
            Row.withSchema(expectedInnerSchema).addValue("one2").build()),
        nestedArray(
            Row.withSchema(expectedInnerSchema).addValue("two1").build(),
            Row.withSchema(expectedInnerSchema).addValue("two2").build()),
        nestedArray(
            Row.withSchema(expectedInnerSchema).addValue("three1").build(),
            Row.withSchema(expectedInnerSchema).addValue("three2").build()));
    PAssert.that(result).containsInAnyOrder(expectedRows);
    pipeline.run();
  }

  @Test
  public void testDropAllFields() {

  }
}
