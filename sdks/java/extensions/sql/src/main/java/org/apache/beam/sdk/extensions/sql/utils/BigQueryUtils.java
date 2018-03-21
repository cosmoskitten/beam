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

package org.apache.beam.sdk.extensions.sql.utils;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Utility methods for BigQuery related operations.
 *
 * <p><b>Example: Writing to BigQuery</b>
 *
 * <pre>{@code
 * PCollection<Row> rows = ...;
 *
 * rows.apply(BigQueryIO.<Row>write()
 *       .withSchema(BigQueryUtils.toTableSchema(rows))
 *       .withFormatFunction(BigQueryUtils.toTableRow())
 *       .to("my-project:my_dataset.my_table"));
 * }</pre>
 */
public class BigQueryUtils {
  private static final Map<FieldType, StandardSQLTypeName> BEAM_TO_BIGQUERY_TYPE_MAPPING =
      ImmutableMap.<FieldType, StandardSQLTypeName>builder()
          .put(TypeName.BYTE.type(), StandardSQLTypeName.INT64)
          .put(TypeName.INT16.type(), StandardSQLTypeName.INT64)
          .put(TypeName.INT32.type(), StandardSQLTypeName.INT64)
          .put(TypeName.INT64.type(), StandardSQLTypeName.INT64)

          .put(TypeName.FLOAT.type(), StandardSQLTypeName.FLOAT64)
          .put(TypeName.DOUBLE.type(), StandardSQLTypeName.FLOAT64)

          .put(TypeName.DECIMAL.type(), StandardSQLTypeName.FLOAT64)

          .put(TypeName.BOOLEAN.type(), StandardSQLTypeName.BOOL)

          .put(TypeName.ARRAY.type(), StandardSQLTypeName.ARRAY)
          .put(TypeName.ROW.type(), StandardSQLTypeName.STRUCT)

          .put(TypeName.DATETIME.type(), StandardSQLTypeName.TIMESTAMP)
          .put(TypeName.STRING.type(), StandardSQLTypeName.STRING)

          .build();

  private static final Map<byte[], StandardSQLTypeName> BEAM_TO_BIGQUERY_METADATA_MAPPING =
      ImmutableMap.<byte[], StandardSQLTypeName>builder()
          .put("DATE".getBytes(), StandardSQLTypeName.DATE)
          .put("TIME".getBytes(), StandardSQLTypeName.TIME)
          .put("TIME_WITH_LOCAL_TZ".getBytes(), StandardSQLTypeName.TIME)
          .put("TS".getBytes(), StandardSQLTypeName.TIMESTAMP)
          .put("TS_WITH_LOCAL_TZ".getBytes(), StandardSQLTypeName.TIMESTAMP)
          .build();

  /**
   * Get the corresponding BigQuery {@link StandardSQLTypeName}
   * for supported Beam {@link FieldType}.
   */
  private static StandardSQLTypeName toStandardSQLTypeName(FieldType fieldType) {
    StandardSQLTypeName sqlType = BEAM_TO_BIGQUERY_TYPE_MAPPING.get(fieldType.getTypeName().type());

    if (sqlType == StandardSQLTypeName.TIMESTAMP && fieldType.getMetadata() != null) {
      sqlType = BEAM_TO_BIGQUERY_METADATA_MAPPING.get(fieldType.getMetadata());
    }

    return sqlType;
  }

  private static List<TableFieldSchema> toTableFieldSchema(Schema schema) {
    List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>(schema.getFieldCount());
    for (Field schemaField : schema.getFields()) {
      FieldType type = schemaField.getType();

      TableFieldSchema field = new TableFieldSchema()
        .setName(schemaField.getName());
      if (schemaField.getDescription() != null && schemaField.getDescription() != "") {
        field.setDescription(schemaField.getDescription());
      }

      if (!schemaField.getNullable()) {
        field.setMode(Mode.REQUIRED.toString());
      }
      if (TypeName.ARRAY.type() == type.getTypeName().type()) {
        type = type.getComponentType();
        field.setMode(Mode.REPEATED.toString());
      }
      if (TypeName.ROW.type() == type.getTypeName().type()) {
        Schema subType = type.getRowSchema();
        field.setFields(toTableFieldSchema(subType));
      }
      field.setType(toStandardSQLTypeName(type).toString());

      fields.add(field);
    }
    return fields;
  }

  /**
   * Convert a Beam {@link Schema} to a BigQuery {@link TableSchema}.
   */
  public static TableSchema toTableSchema(Schema schema) {
    return new TableSchema().setFields(toTableFieldSchema(schema));
  }

  /**
   * Convert a Beam {@link PCollection} to a BigQuery {@link TableSchema}.
   */
  public static TableSchema toTableSchema(PCollection<Row> rows) {
    RowCoder coder = (RowCoder) rows.getCoder();
    return toTableSchema(coder.getSchema());
  }

  private static final SerializableFunction<Row, TableRow> TO_TABLE_ROW = new ToTableRow();

  /**
   * Convert a Beam {@link Row} to a BigQuery {@link TableRow}.
   */
  public static SerializableFunction<Row, TableRow> toTableRow() {
    return TO_TABLE_ROW;
  }

  /**
   * Convert a Beam {@link Row} to a BigQuery {@link TableRow}.
   */
  private static class ToTableRow implements SerializableFunction<Row, TableRow> {
    @Override
    public TableRow apply(Row input) {
      TableRow output = new TableRow();
      for (int i = 0; i < input.getFieldCount(); i++) {
        Object value = input.getValue(i);

        Field schemaField = input.getSchema().getField(i);
        FieldType type = schemaField.getType().getTypeName().type();
        if (TypeName.ARRAY.type() == type) {
          type = schemaField.getType().getComponentType().getTypeName().type();
          if (TypeName.ROW.type() == type) {
            List<Row> rows = (List<Row>) value;
            List<TableRow> tableRows = new ArrayList<TableRow>(rows.size());
            for (int j = 0; j < rows.size(); j++) {
              tableRows.add(apply(rows.get(j)));
            }
            value = tableRows;
          }
        } else if (TypeName.ROW.type() == type) {
          value = apply((Row) value);
        }

        output = output.set(
            schemaField.getName(),
            value);
      }
      return output;
    }
  }
}
