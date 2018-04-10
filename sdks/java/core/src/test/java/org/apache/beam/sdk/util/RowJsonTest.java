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
package org.apache.beam.sdk.util;

import static org.apache.beam.sdk.schemas.Schema.TypeName.ARRAY;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT32;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.util.Arrays;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

/**
 * Unit tests for {@link RowJson}.
 */
public class RowJsonTest {
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();
  private static final boolean NOT_NULLABLE = false;

  @Test
  public void testParsesFlatRow() throws Exception {
    Schema schema =
        Schema
            .builder()
            .addByteField("f_byte", NOT_NULLABLE)
            .addInt16Field("f_int16", NOT_NULLABLE)
            .addInt32Field("f_int32", NOT_NULLABLE)
            .addInt64Field("f_int64", NOT_NULLABLE)
            .addFloatField("f_float", NOT_NULLABLE)
            .addDoubleField("f_double", NOT_NULLABLE)
            .addBooleanField("f_boolean", NOT_NULLABLE)
            .addStringField("f_string", NOT_NULLABLE)
            .build();

    String rowString = "{\n"
                       + "\"f_byte\" : 12,\n"
                       + "\"f_int16\" : 22,\n"
                       + "\"f_int32\" : 32,\n"
                       + "\"f_int64\" : 42,\n"
                       + "\"f_float\" : 52.1,\n"
                       + "\"f_double\" : 62.2,\n"
                       + "\"f_boolean\" : \"true\",\n"
                       + "\"f_string\" : \"hello\"\n"
                       + "}";

    RowJson.Deserializer deserializer = RowJson.deserializerFor(schema);

    Row parsedRow = objectMapperWith(deserializer).readValue(rowString, Row.class);

    Row expectedRow =
        Row
            .withSchema(schema)
            .addValues((byte) 12, (short) 22, 32, (long) 42, 52.1f, 62.2d, true, "hello")
            .build();

    assertEquals(expectedRow, parsedRow);
  }

  @Test
  public void testParsesArrayField() throws Exception {
    Schema schema =
        Schema
            .builder()
            .addInt32Field("f_int32", NOT_NULLABLE)
            .addArrayField("f_intArray", INT32.type())
            .build();

    String rowString = "{\n"
                       + "\"f_int32\" : 32,\n"
                       + "\"f_intArray\" : [ 1, 2, 3, 4, 5]\n"
                       + "}";

    RowJson.Deserializer deserializer = RowJson.deserializerFor(schema);

    Row parsedRow = objectMapperWith(deserializer).readValue(rowString, Row.class);

    Row expectedRow =
        Row
            .withSchema(schema)
            .addValues(32, Arrays.asList(1, 2, 3, 4, 5))
            .build();

    assertEquals(expectedRow, parsedRow);
  }

  @Test
  public void testParsesArrayOfArrays() throws Exception {
    Schema schema =
        Schema
            .builder()
            .addArrayField("f_arrayOfIntArrays",
                           FieldType.of(ARRAY).withComponentType(INT32.type()))
            .build();

    String rowString = "{\n"
                       + "\"f_arrayOfIntArrays\" : [ [1, 2], [3, 4], [5]]\n"
                       + "}";

    RowJson.Deserializer deserializer = RowJson.deserializerFor(schema);

    Row parsedRow = objectMapperWith(deserializer).readValue(rowString, Row.class);

    Row expectedRow =
        Row
            .withSchema(schema)
            .addArray(
                Arrays.asList(1, 2),
                Arrays.asList(3, 4),
                Arrays.asList(5))
            .build();

    assertEquals(expectedRow, parsedRow);
  }

  @Test
  public void testParsesRowField() throws Exception {
    Schema nestedRowSchema =
        Schema
            .builder()
            .addInt32Field("f_nestedInt32", NOT_NULLABLE)
            .addStringField("f_nestedString", NOT_NULLABLE)
            .build();

    Schema schema =
        Schema
            .builder()
            .addInt32Field("f_int32", NOT_NULLABLE)
            .addRowField("f_row", nestedRowSchema, NOT_NULLABLE)
            .build();

    String rowString = "{\n"
                       + "\"f_int32\" : 32,\n"
                       + "\"f_row\" : {\n"
                       + "             \"f_nestedInt32\" : 54,\n"
                       + "             \"f_nestedString\" : \"foo\"\n"
                       + "            }\n"
                       + "}";

    RowJson.Deserializer deserializer = RowJson.deserializerFor(schema);

    Row parsedRow = objectMapperWith(deserializer).readValue(rowString, Row.class);

    Row expectedRow =
        Row
            .withSchema(schema)
            .addValues(32, Row.withSchema(nestedRowSchema).addValues(54, "foo").build())
            .build();

    assertEquals(expectedRow, parsedRow);
  }

  @Test
  public void testParsesNestedRowField() throws Exception {

    Schema doubleNestedRowSchema =
        Schema
            .builder()
            .addStringField("f_doubleNestedString", NOT_NULLABLE)
            .build();

    Schema nestedRowSchema =
        Schema
            .builder()
            .addRowField("f_nestedRow", doubleNestedRowSchema, NOT_NULLABLE)
            .build();

    Schema schema =
        Schema
            .builder()
            .addRowField("f_row", nestedRowSchema, NOT_NULLABLE)
            .build();

    String rowString = "{\n"
                       + "\"f_row\" : {\n"
                       + "             \"f_nestedRow\" : {\n"
                       + "                                \"f_doubleNestedString\":\"foo\"\n"
                       + "                               }\n"
                       + "            }\n"
                       + "}";

    RowJson.Deserializer deserializer = RowJson.deserializerFor(schema);

    Row parsedRow = objectMapperWith(deserializer).readValue(rowString, Row.class);

    Row expectedRow =
        Row
            .withSchema(schema)
            .addValues(
                Row
                    .withSchema(nestedRowSchema)
                    .addValues(
                        Row
                            .withSchema(doubleNestedRowSchema)
                            .addValues("foo")
                            .build())
                    .build())
            .build();

    assertEquals(expectedRow, parsedRow);
  }

  private ObjectMapper objectMapperWith(RowJson.Deserializer deserializer) {
    SimpleModule simpleModule = new SimpleModule("rowSerializationTesModule");
    simpleModule.addDeserializer(Row.class, deserializer);
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(simpleModule);
    return objectMapper;
  }

}
