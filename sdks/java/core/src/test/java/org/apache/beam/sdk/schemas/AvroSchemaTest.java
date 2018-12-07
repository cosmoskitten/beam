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
import org.apache.avro.data.TimeConversions;
import org.apache.avro.specific.SpecificData;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.DateTime;
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
  static final DateTime DATE_TIME = new DateTime().withDate(1979, 03, 14).withTime(1, 2, 3, 4);
  static final TestAvroNested AVRO_NESTED_SPECIFIC_RECORD = new TestAvroNested(true, 42);
  static final TestAvro AVRO_SPECIFIC_RECORD =
      new TestAvro(
          true,
          43,
          44L,
          (float) 44.1,
          (double) 44.2,
          "mystring",
          ByteBuffer.wrap(BYTE_ARRAY),
          DATE_TIME,
          AVRO_NESTED_SPECIFIC_RECORD,
          ImmutableList.of(AVRO_NESTED_SPECIFIC_RECORD, AVRO_NESTED_SPECIFIC_RECORD),
          ImmutableMap.of("k1", AVRO_NESTED_SPECIFIC_RECORD, "k2", AVRO_NESTED_SPECIFIC_RECORD));

  static final Row NESTED_ROW = Row.withSchema(SUBSCHEMA).addValues(true, 42).build();
  static final Row ROW =
      Row.withSchema(SCHEMA)
          .addValues(
              true,
              43,
              44L,
              (float) 44.1,
              (double) 44.2,
              "mystring",
              ByteBuffer.wrap(BYTE_ARRAY),
              DATE_TIME,
              NESTED_ROW,
              ImmutableList.of(NESTED_ROW, NESTED_ROW),
              ImmutableMap.of("k1", NESTED_ROW, "k2", NESTED_ROW))
          .build();

  @Test
  public void testSpecificRecordToRow() {
    SerializableFunction<TestAvro, Row> toRow =
        new AvroSpecificRecordSchema().toRowFunction(TypeDescriptor.of(TestAvro.class));
    assertEquals(ROW, toRow.apply(AVRO_SPECIFIC_RECORD));
  }

  @Test
  public void testRowToSpecificRecord() {
    SerializableFunction<Row, TestAvro> fromRow =
        new AvroSpecificRecordSchema().fromRowFunction(TypeDescriptor.of(TestAvro.class));
    // This works around a bug in the Avro library (AVRO-1891) around SpecificRecord's handling
    // of DateTime types.
    SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampConversion());
    assertEquals(AVRO_SPECIFIC_RECORD, fromRow.apply(ROW));
  }
}
