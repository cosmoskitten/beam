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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

public class AvroSchemaTest {
  private static final Schema SUBSCHEMA =
      Schema.builder()
          .addField("bool_non_nullable", FieldType.BOOLEAN)
          .addNullableField("int", FieldType.INT32)
          .build();
  private static final FieldType SUB_TYPE = FieldType.row(SUBSCHEMA).withNullable(true);

  private static final Schema SCHEMA =
      Schema.builder()
          .addField("bool_non_nullable", FieldType.BOOLEAN)
          .addNullableField("int", FieldType.INT32)
          .addNullableField("long", FieldType.INT64)
          .addNullableField("float", FieldType.FLOAT)
          .addNullableField("double", FieldType.DOUBLE)
          .addNullableField("string", FieldType.STRING)
          .addNullableField("bytes", FieldType.BYTES)
          .addNullableField("timestampMillis", FieldType.DATETIME)
          .addNullableField("row", SUB_TYPE)
          .addNullableField("array", FieldType.array(SUB_TYPE))
          .addNullableField("map", FieldType.map(FieldType.STRING, SUB_TYPE))
          .build();

  @Test
  public void testSpecificRecordSchema() {
    assertEquals(
        SCHEMA, new AvroSpecificRecordSchema().schemaFor(TypeDescriptor.of(TestAvro.class)));
  }

  static final byte[] BYTE_ARRAY = new byte[] {1, 2, 3, 4};
  static final DateTime DATE_TIME =
      new DateTime().withDate(1979, 03, 14).withTime(1, 2, 3, 4).withZone(DateTimeZone.UTC);

  @Test
  public void testSpecificRecordToRow() {
    TestAvroNested nested = new TestAvroNested(true, 42);
    TestAvro outer =
        new TestAvro(
            true,
            43,
            44L,
            (float) 44.1,
            (double) 44.2,
            "mystring",
            ByteBuffer.wrap(BYTE_ARRAY),
            DATE_TIME,
            nested,
            ImmutableList.of(nested, nested),
            ImmutableMap.of("k1", nested, "k2", nested));
    SerializableFunction<TestAvro, Row> toRow =
        new AvroSpecificRecordSchema().toRowFunction(TypeDescriptor.of(TestAvro.class));
    System.out.println(toRow.apply(outer));
  }

  @Test
  public void testRowToSpecificRecord() {
    Schema nestedSchema =
        new AvroSpecificRecordSchema().schemaFor(TypeDescriptor.of(TestAvroNested.class));
    Row nested = Row.withSchema(nestedSchema).addValues(true, 42).build();

    Schema schema = new AvroSpecificRecordSchema().schemaFor(TypeDescriptor.of(TestAvro.class));
    Row row =
        Row.withSchema(schema)
            .addValues(
                true,
                43,
                44L,
                (float) 44.1,
                (double) 44.2,
                "mystring",
                ByteBuffer.wrap(BYTE_ARRAY),
                DATE_TIME,
                nested,
                ImmutableList.of(nested, nested),
                ImmutableMap.of("k1", nested, "k2", nested))
            .build();

    SerializableFunction<Row, TestAvro> fromRow =
        new AvroSpecificRecordSchema().fromRowFunction(TypeDescriptor.of(TestAvro.class));
    System.out.println(fromRow.apply(row));
  }
}
