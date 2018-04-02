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

package org.apache.beam.sdk.extensions.sql.impl.utils;

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.Schema.FieldTypeDescriptor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Utility methods for Calcite related operations.
 */
public class CalciteUtils {
  private static final long UNLIMITED_ARRAY_SIZE = -1L;
  // Beam's Schema class has a single DATETIME type, so we need a way to distinguish the different
  // Calcite time classes. We do this by storing extra metadata in the FieldTypeDescriptor so we
  // can tell which time class this is.
  //
  // Same story with CHAR and VARCHAR - they both map to STRING.
  private static final BiMap<Schema.FieldTypeDescriptor, SqlTypeName> BEAM_TO_CALCITE_TYPE_MAPPING =
      ImmutableBiMap.<Schema.FieldTypeDescriptor, SqlTypeName>builder()
          .put(TypeName.BYTE.typeDescriptor(), SqlTypeName.TINYINT)
          .put(TypeName.INT16.typeDescriptor(), SqlTypeName.SMALLINT)
          .put(TypeName.INT32.typeDescriptor(), SqlTypeName.INTEGER)
          .put(TypeName.INT64.typeDescriptor(), SqlTypeName.BIGINT)

          .put(TypeName.FLOAT.typeDescriptor(), SqlTypeName.FLOAT)
          .put(TypeName.DOUBLE.typeDescriptor(), SqlTypeName.DOUBLE)

          .put(TypeName.DECIMAL.typeDescriptor(), SqlTypeName.DECIMAL)

          .put(TypeName.BOOLEAN.typeDescriptor(), SqlTypeName.BOOLEAN)

          .put(TypeName.ARRAY.typeDescriptor(), SqlTypeName.ARRAY)
          .put(TypeName.ROW.typeDescriptor(), SqlTypeName.ROW)
          .put(TypeName.DATETIME.typeDescriptor()
                  .withMetadata("DATE".getBytes(StandardCharsets.UTF_8)),
              SqlTypeName.DATE)
          .put(TypeName.DATETIME.typeDescriptor()
                  .withMetadata("TIME".getBytes(StandardCharsets.UTF_8)),
              SqlTypeName.TIME)
          .put(TypeName.DATETIME.typeDescriptor()
                  .withMetadata("TIME_WITH_LOCAL_TZ".getBytes(StandardCharsets.UTF_8)),
              SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
          .put(TypeName.DATETIME.typeDescriptor()
                  .withMetadata("TS".getBytes(StandardCharsets.UTF_8)),
              SqlTypeName.TIMESTAMP)
          .put(TypeName.DATETIME.typeDescriptor()
                  .withMetadata("TS_WITH_LOCAL_TZ".getBytes(StandardCharsets.UTF_8)),
              SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
          .put(TypeName.STRING.typeDescriptor()
                  .withMetadata("CHAR".getBytes(StandardCharsets.UTF_8)),
              SqlTypeName.CHAR)
          .put(TypeName.STRING.typeDescriptor()
                  .withMetadata("VARCHAR".getBytes(StandardCharsets.UTF_8)),
              SqlTypeName.VARCHAR)
          .build();
  private static final BiMap<SqlTypeName, Schema.FieldTypeDescriptor> CALCITE_TO_BEAM_TYPE_MAPPING =
      BEAM_TO_CALCITE_TYPE_MAPPING.inverse();

  // Since there are multiple Calcite type that correspond to a single Beam type, this is the
  // default mapping.
  private static final Map<FieldTypeDescriptor, SqlTypeName>
      BEAM_TO_CALCITE_DEFAULT_MAPPING = ImmutableMap.of(
          TypeName.DATETIME.typeDescriptor(), SqlTypeName.TIMESTAMP,
          TypeName.STRING.typeDescriptor(), SqlTypeName.VARCHAR);

  /**
   * Generate {@code BeamSqlRowType} from {@code RelDataType} which is used to create table.
   */
  public static Schema toBeamSchema(RelDataType tableInfo) {
    return
        tableInfo
            .getFieldList()
            .stream()
            .map(CalciteUtils::toBeamSchemaField)
            .collect(toSchema());
  }

  public static SqlTypeName toSqlTypeName(FieldTypeDescriptor typeDescriptor) {
    SqlTypeName typeName = BEAM_TO_CALCITE_TYPE_MAPPING.get(
        typeDescriptor.withComponentType(null).withRowSchema(null));
    if (typeName != null) {
      return typeName;
    } else {
      // This will happen e.g. if looking up a STRING type, and metadata isn't set to say which
      // type of SQL string we want. In this case, use the default mapping.
      return BEAM_TO_CALCITE_DEFAULT_MAPPING.get(typeDescriptor);
    }
  }

  public static Schema.FieldTypeDescriptor toFieldTypeDescriptor(SqlTypeName sqlTypeName) {
    return CALCITE_TO_BEAM_TYPE_MAPPING.get(sqlTypeName).getType().typeDescriptor();
  }

  public static Schema.FieldTypeDescriptor toFieldTypeDescriptor(RelDataType calciteType) {
    FieldTypeDescriptor typeDescriptor = toFieldTypeDescriptor((calciteType.getSqlTypeName()));
    if (calciteType.getComponentType() != null) {
      typeDescriptor = typeDescriptor.withComponentType(toFieldTypeDescriptor(
          calciteType.getComponentType()));
    }
    if (calciteType.isStruct()) {
      typeDescriptor = typeDescriptor.withRowSchema(toBeamSchema(calciteType));
    }
    return typeDescriptor;
  }

  public static Schema.FieldTypeDescriptor toArrayTypeDescriptor(SqlTypeName componentType) {
    return TypeName.ARRAY.typeDescriptor().withComponentType(toFieldTypeDescriptor(componentType));
  }

  public static Schema.FieldTypeDescriptor toArrayTypeDescriptor(RelDataType componentType) {
    return TypeName.ARRAY.typeDescriptor().withComponentType(toFieldTypeDescriptor(componentType));
  }

  public static Schema.Field toBeamSchemaField(RelDataTypeField calciteField) {
    Schema.FieldTypeDescriptor fieldTypeDescriptor = toFieldTypeDescriptor(calciteField.getType());
    // TODO: We should support Calcite's nullable annotations.
    return Schema.Field.of(calciteField.getName(), fieldTypeDescriptor)
        .withNullable(true);
  }

  /**
   * Create an instance of {@code RelDataType} so it can be used to create a table.
   */
  public static RelDataType toCalciteRowType(Schema schema, RelDataTypeFactory dataTypeFactory) {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(dataTypeFactory);

    IntStream
        .range(0, schema.getFieldCount())
        .forEach(idx ->
                     builder.add(
                         schema.getField(idx).getName(),
                         toRelDataType(dataTypeFactory, schema, idx)));
    return builder.build();
  }

  private static RelDataType toRelDataType(
      RelDataTypeFactory dataTypeFactory, Schema.FieldTypeDescriptor fieldTypeDescriptor) {
    SqlTypeName typeName = toSqlTypeName(fieldTypeDescriptor);
    if (SqlTypeName.ARRAY.equals(typeName)) {
      RelDataType componentType = toRelDataType(
          dataTypeFactory, fieldTypeDescriptor.getComponentType());
      return dataTypeFactory.createArrayType(componentType, UNLIMITED_ARRAY_SIZE);
    } else if (SqlTypeName.ROW.equals(typeName)) {
      return toCalciteRowType(fieldTypeDescriptor.getRowSchema(), dataTypeFactory);
    } else {
      return dataTypeFactory.createSqlType(typeName);
    }
  }

  private static RelDataType toRelDataType(
      RelDataTypeFactory dataTypeFactory,
      Schema schema,
      int fieldIndex) {
    Schema.Field field = schema.getField(fieldIndex);
    return toRelDataType(dataTypeFactory, field.getTypeDescriptor());
  }
}
