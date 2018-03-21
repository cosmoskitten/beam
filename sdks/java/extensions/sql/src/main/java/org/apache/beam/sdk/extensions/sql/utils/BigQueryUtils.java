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
import org.apache.beam.sdk.extensions.sql.SqlTypeCoder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoders;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;

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
  private static final Map<SqlTypeCoder, StandardSQLTypeName> BEAM_TO_BIGQUERY_TYPE_MAPPING =
      ImmutableMap.<SqlTypeCoder, StandardSQLTypeName>builder()
          .put(SqlTypeCoders.TINYINT, StandardSQLTypeName.INT64)
          .put(SqlTypeCoders.SMALLINT, StandardSQLTypeName.INT64)
          .put(SqlTypeCoders.INTEGER, StandardSQLTypeName.INT64)
          .put(SqlTypeCoders.BIGINT, StandardSQLTypeName.INT64)

          .put(SqlTypeCoders.FLOAT, StandardSQLTypeName.FLOAT64)
          .put(SqlTypeCoders.DOUBLE, StandardSQLTypeName.FLOAT64)

          .put(SqlTypeCoders.DECIMAL, StandardSQLTypeName.FLOAT64)

          .put(SqlTypeCoders.CHAR, StandardSQLTypeName.STRING)
          .put(SqlTypeCoders.VARCHAR, StandardSQLTypeName.STRING)

          .put(SqlTypeCoders.DATE, StandardSQLTypeName.DATE)
          .put(SqlTypeCoders.TIME, StandardSQLTypeName.TIME)
          .put(SqlTypeCoders.TIMESTAMP, StandardSQLTypeName.TIMESTAMP)

          .put(SqlTypeCoders.BOOLEAN, StandardSQLTypeName.BOOL)
          .build();

  /**
   * Get the corresponding BigQuery {@link StandardSQLTypeName}
   * for supported Beam SQL type coder, see {@link SqlTypeCoder}.
   */
  private static StandardSQLTypeName toStandardSQLTypeName(SqlTypeCoder coder) {
    if (SqlTypeCoders.isArray(coder)) {
        return StandardSQLTypeName.ARRAY;
    }

    if (SqlTypeCoders.isRow(coder)) {
      return StandardSQLTypeName.STRUCT;
    }

    return BEAM_TO_BIGQUERY_TYPE_MAPPING.get(coder);
  }

  private static List<TableFieldSchema> toTableFieldSchema(RowType type) {
    List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>(type.getFieldCount());
    for (int i = 0; i < type.getFieldCount(); i++) {
      TableFieldSchema field = new TableFieldSchema()
          .setName(type.getFieldName(i));

      SqlTypeCoder coder = (SqlTypeCoder) type.getFieldCoder(i);
      if (SqlTypeCoders.isArray(coder)) {
        field.setMode(Mode.REPEATED.toString());
        coder = ((SqlTypeCoder.SqlArrayCoder) coder).getElementCoder();
      }
      if (SqlTypeCoders.isRow(coder)) {
        RowType subType = ((SqlTypeCoder.SqlRowCoder) coder).getRowType();
        field.setFields(toTableFieldSchema(subType));
      }
      field.setType(toStandardSQLTypeName(coder).toString());

      fields.add(field);
    }
    return fields;
  }

  /**
   * Convert a Beam {@link RowType} to a BigQuery {@link TableSchema}.
   */
  public static TableSchema toTableSchema(RowType type) {
    return new TableSchema().setFields(toTableFieldSchema(type));
  }

  /**
   * Convert a Beam {@link PCollection} to a BigQuery {@link TableSchema}.
   */
  public static TableSchema toTableSchema(PCollection<Row> rows) {
    RowCoder coder = (RowCoder) rows.getCoder();
    return toTableSchema(coder.getRowType());
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

        SqlTypeCoder coder = (SqlTypeCoder) input.getRowType().getFieldCoder(i);
        if (SqlTypeCoders.isArray(coder)) {
          coder = ((SqlTypeCoder.SqlArrayCoder) coder).getElementCoder();
          if (SqlTypeCoders.isRow(coder)) {
            List<Row> rows = (List<Row>) value;
            List<TableRow> tableRows = new ArrayList<TableRow>(rows.size());
            for (int j = 0; j < rows.size(); j++) {
              tableRows.add(apply(rows.get(j)));
            }
            value = tableRows;
          }
        } else if (SqlTypeCoders.isRow(coder)) {
          value = apply((Row) value);
        }

        output = output.set(
            input.getRowType().getFieldName(i),
            value);
      }
      return output;
    }
  }
}
