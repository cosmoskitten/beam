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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.schemas.Schema.TypeName.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.TypeName.BYTE;
import static org.apache.beam.sdk.schemas.Schema.TypeName.DOUBLE;
import static org.apache.beam.sdk.schemas.Schema.TypeName.FLOAT;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT16;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT32;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT64;
import static org.apache.beam.sdk.schemas.Schema.TypeName.STRING;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/**
 * Helpers for serializing/deserializing {@link Row Rows} to/from JSON.
 */
public class RowJson {
  private static final Map<Schema.TypeName, Function<JsonNode, ?>> VALUE_GETTERS =
      ImmutableMap.<Schema.TypeName, Function<JsonNode, ?>> builder()
          .put(BYTE, node -> (byte) node.asInt())
          .put(INT16, node -> (short) node.asInt())
          .put(INT32, node -> node.asInt())
          .put(INT64, node -> node.asLong())
          .put(FLOAT, node -> (float) node.asDouble())
          .put(DOUBLE, node -> node.asDouble())
          .put(BOOLEAN, node -> node.asBoolean())
          .put(STRING, node -> node.asText())
          .build();

  private RowJson() { }

  /**
   * Creates a Jackson deserializer for a {@link Schema}.
   */
  public static Deserializer deserializerFor(Schema schema) {
    return Deserializer.forSchema(schema);
  }

  /**
   * Jackson deserializer for {@link Row Rows}.
   */
  public static class Deserializer extends StdDeserializer<Row> {

    private Schema schema;

    /**
     * Creates a deserializer for a row {@link Schema}.
     */
    public static Deserializer forSchema(Schema schema) {
      schema.getFields().forEach(Deserializer::verifyFieldTypeSupported);
      return new Deserializer(schema);
    }

    private static void verifyFieldTypeSupported(Schema.Field field) {
      Schema.FieldType fieldType = field.getType();
      verifyFieldTypeSupported(fieldType);
    }

    private static void verifyFieldTypeSupported(Schema.FieldType fieldType) {
      Schema.TypeName fieldTypeName = fieldType.getTypeName();

      if (fieldTypeName.isCompositeType()) {
        Schema rowFieldSchema = fieldType.getRowSchema();
        rowFieldSchema.getFields().forEach(Deserializer::verifyFieldTypeSupported);
        return;
      }

      if (fieldTypeName.isContainerType()) {
        verifyFieldTypeSupported(fieldType.getComponentType());
        return;
      }

      if (!VALUE_GETTERS.containsKey(fieldTypeName)) {
        throw new IllegalArgumentException(
            "ToRow transform does not support Row fields of type " + fieldTypeName.name());
      }
    }

    private Deserializer(Schema schema) {
      super(Row.class);
      this.schema = schema;
    }

    @Override
    public Row deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException {

      JsonNode rootObject = jsonParser.readValueAsTree();

      if (!rootObject.isObject()) {
        throw new UnsupportedRowJsonException(
            "Unable to convert '" + rootObject.asText() + "' to Beam Row, it is not a JSON object. "
            + "Only JSON objects can be parsed to Beam Rows at the moment");
      }

      return nodeToRow(rootObject, schema);
    }
  }

  private static Row nodeToRow(
      JsonNode jsonNode,
      Schema targetRowSchema) {
    return
        IntStream
            .range(0, targetRowSchema.getFieldCount())
            .mapToObj(i -> getFieldValue(jsonNode, targetRowSchema, i))
            .collect(Row.toRow(targetRowSchema));
  }

  private static Object getFieldValue(
      JsonNode currentRowNode,
      Schema currentRowSchema,
      int fieldIndex) {

    String fieldName = currentRowSchema.nameOf(fieldIndex);
    Schema.FieldType fieldType = currentRowSchema.getField(fieldIndex).getType();
    JsonNode fieldValueJson = currentRowNode.get(fieldName);

    return getFieldValue(fieldValueJson, fieldName, fieldType);
  }

  private static Object getFieldValue(
      JsonNode currentValueNode,
      String currentFieldName,
      Schema.FieldType currentFieldType) {

    Schema.TypeName fieldTypeName = currentFieldType.getTypeName();

    if (currentValueNode == null) {
      throw new UnsupportedRowJsonException(
          "Field " + currentFieldName + " is not present in the JSON object");
    }

    if (currentValueNode.isNull()) {
      return null;
    }

    if (fieldTypeName.isContainerType()) {
      return
          arrayNodeToList(
              currentValueNode,
              currentFieldName,
              currentFieldType);
    }

    if (fieldTypeName.isCompositeType()) {
      return
          nodeToRow(
              currentValueNode,
              currentFieldType.getRowSchema());
    }

    return
        getPrimitiveValue(
            currentValueNode,
            currentFieldName,
            currentFieldType.getTypeName());
  }

  private static Object arrayNodeToList(
      JsonNode currentArrayNode,
      String currentArrayFieldName,
      Schema.FieldType arrayFieldType) {

    if (!currentArrayNode.isArray()) {
      throw new UnsupportedRowJsonException(
          "Expected array for field " + currentArrayFieldName + ". "
          + "Instead got " + currentArrayNode.getNodeType().name());
    }

    return
        IntStream
            .range(0, currentArrayNode.size())
            .mapToObj(i ->
                          getFieldValue(
                              currentArrayNode.get(i),
                              currentArrayFieldName + "[" + i + "]",
                              arrayFieldType.getComponentType()))
            .collect(toList());
  }

  private static Object getPrimitiveValue(
      JsonNode jsonNode,
      String fieldName,
      Schema.TypeName fieldTypeName) {

    try {
      return VALUE_GETTERS.get(fieldTypeName).apply(jsonNode);
    } catch (RuntimeException e) {
      throw new UnsupportedRowJsonException(
          "Unable to get value from field '" + fieldName
          + "'. Expected type " + fieldTypeName.getClass().getSimpleName()
          + ". JSON node is of " + jsonNode.getNodeType().name(), e);
    }
  }


  /**
   * Gets thrown when Row parsing fails for any reason.
   */
  public static class UnsupportedRowJsonException extends RuntimeException {

    private UnsupportedRowJsonException(String message, Throwable reason) {
      super(message, reason);
    }

    private UnsupportedRowJsonException(String message) {
      super(message);
    }
  }
}
