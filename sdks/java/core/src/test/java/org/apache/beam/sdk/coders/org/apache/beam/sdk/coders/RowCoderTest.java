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

package org.apache.beam.sdk.coders.org.apache.beam.sdk.coders;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldTypeDescriptor;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

/**
 * Unit tests for {@link RowCoder}.
 */
public class RowCoderTest {

  void checkEncodeDecode(Row row) throws IOException {
    RowCoder coder = RowCoder.of(row.getSchema());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    coder.encode(row, out);
    assertEquals(row, coder.decode(new ByteArrayInputStream(out.toByteArray())));
  }

  @Test
  public void testPrimitiveTypes() throws Exception {
    Schema schema = Schema.of(Lists.newArrayList(
        Field.of("f_byte", TypeName.BYTE.typeDescriptor()),
        Field.of("f_int16", TypeName.INT16.typeDescriptor()),
        Field.of("f_int32", TypeName.INT32.typeDescriptor()),
        Field.of("f_int64", TypeName.INT64.typeDescriptor()),
        Field.of("f_decimal", TypeName.DECIMAL.typeDescriptor()),
        Field.of("f_float", TypeName.FLOAT.typeDescriptor()),
        Field.of("f_double", TypeName.DOUBLE.typeDescriptor()),
        Field.of("f_string", TypeName.STRING.typeDescriptor()),
        Field.of("f_datetime", TypeName.DATETIME.typeDescriptor()),
        Field.of("f_boolean", TypeName.BOOLEAN.typeDescriptor())));

    DateTime dateTime = new DateTime().withDate(1979, 03, 14)
        .withTime(1, 2, 3, 4)
        .withZone(DateTimeZone.UTC);
    Row row =
        Row
            .withSchema(schema)
            .addValues((byte) 0, (short) 1, 2, 3L, new BigDecimal(2.3), 1.2f, 3.0d, "str",
                dateTime, false)
            .build();
    checkEncodeDecode(row);
  }

  @Test
  public void testNestedTypes() throws Exception {
    Schema nestedSchema = Schema.of(
        Field.of("f1_int", TypeName.INT32.typeDescriptor()),
        Field.of("f1_str", TypeName.STRING.typeDescriptor()));
    Schema schema = Schema.of(
        Field.of("f_int", TypeName.INT32.typeDescriptor()),
        Field.of("nested", TypeName.ROW.typeDescriptor().withRowSchema(nestedSchema)));

    Row nestedRow = Row.withSchema(nestedSchema).addValues(18, "foobar").build();
    Row row = Row.withSchema(schema).addValues(42, nestedRow).build();
    checkEncodeDecode(row);
  }

  @Test
  public void testArrays() throws Exception {
    FieldTypeDescriptor arrayType = TypeName.ARRAY.typeDescriptor()
        .withComponentType(TypeName.STRING.typeDescriptor());
    Schema schema = Schema.of(Field.of("f_array", arrayType));
    Row row = Row.withSchema(schema).addArray("one", "two", "three", "four").build();
    checkEncodeDecode(row);
  }

  @Test
  public void testArrayOfRow() throws Exception {
    Schema nestedSchema = Schema.of(
        Field.of("f1_int", TypeName.INT32.typeDescriptor()),
        Field.of("f1_str", TypeName.STRING.typeDescriptor()));
    FieldTypeDescriptor arrayType = TypeName.ARRAY.typeDescriptor()
        .withComponentType(TypeName.ROW.typeDescriptor()
            .withRowSchema(nestedSchema));
    Schema schema = Schema.of(Field.of("f_array", arrayType));
    Row row = Row.withSchema(schema).addArray(
        Row.withSchema(nestedSchema).addValues(1, "one").build(),
        Row.withSchema(nestedSchema).addValues(2, "two").build(),
        Row.withSchema(nestedSchema).addValues(3, "three").build())
        .build();
    checkEncodeDecode(row);
  }

  @Test
  public void testArrayOfArray() throws Exception {
    FieldTypeDescriptor arrayType = TypeName.ARRAY.typeDescriptor()
        .withComponentType(TypeName.ARRAY.typeDescriptor()
            .withComponentType(TypeName.INT32.typeDescriptor()));
    Schema schema = Schema.of(Field.of("f_array", arrayType));
    Row row = Row.withSchema(schema).addArray(
        Lists.newArrayList(1, 2, 3, 4),
        Lists.newArrayList(5, 6, 7, 8),
        Lists.newArrayList(9, 10, 11, 12)).build();
    checkEncodeDecode(row);
  }
}
