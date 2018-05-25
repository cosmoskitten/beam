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
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

/** Utility methods for Calcite related operations. */
public class CalciteUtils {
  private static final long UNLIMITED_ARRAY_SIZE = -1L;

  // Beam's Schema class has a single DATETIME type, so we need a way to distinguish the different
  // Calcite time classes. We do this by storing extra metadata in the FieldType so we
  // can tell which time class this is.
  //
  // Same story with CHAR and VARCHAR - they both map to STRING.
  private static final BiMap<Schema.TypeName, SqlTypeName> BEAM_TO_CALCITE_TYPE_NAME_MAPPING =
      ImmutableBiMap.<SqlTypeName, Schema.FieldType>builder()
          .put(SqlTypeName.TINYINT, Schema.FieldType.BYTE)
          .put(SqlTypeName.SMALLINT, Schema.FieldType.INT16)
          .put(SqlTypeName.INTEGER, Schema.FieldType.INT32)
          .put(SqlTypeName.BIGINT, Schema.FieldType.INT64)
          .put(FLOAT, SqlTypeName.FLOAT, Schema.FieldType.FLOAT)
          .put(Schema.TypeName.DOUBLE, SqlTypeName.DOUBLE, Schema.FieldType.DOUBLE)
          .put(Schema.TypeName.DECIMAL, SqlTypeName.DECIMAL, Schema.TypeName.DECIMAL)
          .put(Schema.TypeName.BOOLEAN, SqlTypeName.BOOLEAN, Schema.TypeName.BOOLEAN)
          .put(Schema.TypeName.MAP, SqlTypeName.MAP)
          .put(Schema.TypeName.ARRAY, SqlTypeName.ARRAY)
          .put(Schema.TypeName.ROW, SqlTypeName.ROW)
          .put(Schema.TypeName.DATETIME.withMetadata("DATE"), SqlTypeName.DATE)
          .put(Schema.TypeName.DATETIME.withMetadata("TIME"), SqlTypeName.TIME)
          .put(
              Schema.TypeName.DATETIME.withMetadata("TIME_WITH_LOCAL_TZ"),
              SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
          .put(FieldType.DATETIME.withMetadata("TS"), SqlTypeName.TIMESTAMP)
          .put(
              FieldType.DATETIME.withMetadata("TS_WITH_LOCAL_TZ"),
              SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
          .put(FieldType.STRING.withMetadata("CHAR"), SqlTypeName.CHAR)
          .put(FieldType.STRING.withMetadata("VARCHAR"), SqlTypeName.VARCHAR)
          .build();
  private static final BiMap<SqlTypeName, FieldType> CALCITE_TO_BEAM_TYPE_MAPPING =
      BEAM_TO_CALCITE_TYPE_NAME_MAPPING.inverse();

  // Since there are multiple Calcite type that correspond to a single Beam type, this is the
  // default mapping.
  private static final Map<FieldType, SqlTypeName> BEAM_TO_CALCITE_DEFAULT_MAPPING =
      ImmutableMap.of(
          FieldType.DATETIME, SqlTypeName.TIMESTAMP,
          FieldType.STRING, SqlTypeName.VARCHAR);

  /** Generate {@code BeamSqlRowType} from {@code RelDataType} which is used to create table. */
  public static Schema toBeamSchema(RelDataType tableInfo) {
    return tableInfo
        .getFieldList()
        .stream()
        .map(CalciteUtils::toBeamSchemaField)
        .collect(toSchema());
  }

  public static SqlTypeName toSqlTypeName(FieldType type) {
    SqlTypeName typeName =
        BEAM_TO_CALCITE_TYPE_NAME_MAPPING.get(
            type.withCollectionElementType(null).withRowSchema(null).withMapType(null, null));
    if (typeName != null) {
      return typeName;
    } else {
      // This will happen e.g. if looking up a STRING type, and metadata isn't set to say which
      // type of SQL string we want. In this case, use the default mapping.
      return BEAM_TO_CALCITE_DEFAULT_MAPPING.get(type);
    }
  }

  public static FieldType toFieldType(SqlTypeName sqlTypeName) {
    return CALCITE_TO_BEAM_TYPE_MAPPING.get(sqlTypeName);
  }

  public static FieldType toFieldType(RelDataType calciteType) {
    FieldType type = toFieldType((calciteType.getSqlTypeName()));
    if (calciteType.getComponentType() != null) {
      type = type.withCollectionElementType(toFieldType(calciteType.getComponentType()));
    }
    if (calciteType.isStruct()) {
      type = type.withRowSchema(toBeamSchema(calciteType));
    }
    if (calciteType.getKeyType() != null && calciteType.getValueType() != null) {
      type =
          type.withMapType(
              toFieldType(calciteType.getKeyType()), toFieldType(calciteType.getValueType()));
    }
    return type;
  }

  public static FieldType toArrayType(SqlTypeName collectionElementType) {
    return FieldType.array(toFieldType(collectionElementType));
  }

  public static FieldType toArrayType(RelDataType collectionElementType) {
    return FieldType.array(toFieldType(collectionElementType));
  }

  public static FieldType toMapType(SqlTypeName componentKeyType, SqlTypeName componentValueType) {
    return FieldType.map(toFieldType(componentKeyType), toFieldType(componentValueType));
  }

  public static FieldType toMapType(RelDataType componentKeyType, RelDataType componentValueType) {
    return FieldType.map(toFieldType(componentKeyType), toFieldType(componentValueType));
  }

  public static Schema.Field toBeamSchemaField(RelDataTypeField calciteField) {
    FieldType fieldType = toFieldType(calciteField.getType());
    // TODO: We should support Calcite's nullable annotations.
    return Schema.Field.of(calciteField.getName(), fieldType).withNullable(true);
  }

  /** Create an instance of {@code RelDataType} so it can be used to create a table. */
  public static RelDataType toCalciteRowType(Schema schema, RelDataTypeFactory dataTypeFactory) {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(dataTypeFactory);

    IntStream.range(0, schema.getFieldCount())
        .forEach(
            idx ->
                builder.add(
                    schema.getField(idx).getName(), toRelDataType(dataTypeFactory, schema, idx)));
    return builder.build();
  }

  private static RelDataType toRelDataType(
      RelDataTypeFactory dataTypeFactory, FieldType fieldType) {
    SqlTypeName typeName = toSqlTypeName(fieldType);
    if (SqlTypeName.ARRAY.equals(typeName)) {
      RelDataType collectionElementType =
          toRelDataType(dataTypeFactory, fieldType.getCollectionElementType());
      return dataTypeFactory.createArrayType(collectionElementType, UNLIMITED_ARRAY_SIZE);
    } else if (SqlTypeName.MAP.equals(typeName)) {
      RelDataType componentKeyType = toRelDataType(dataTypeFactory, fieldType.getMapKeyType());
      RelDataType componentValueType = toRelDataType(dataTypeFactory, fieldType.getMapValueType());
      return dataTypeFactory.createMapType(componentKeyType, componentValueType);
    } else if (SqlTypeName.ROW.equals(typeName)) {
      return toCalciteRowType(fieldType.getRowSchema(), dataTypeFactory);
    } else {
      return dataTypeFactory.createSqlType(typeName);
    }
  }

  private static RelDataType toRelDataType(
      RelDataTypeFactory dataTypeFactory, Schema schema, int fieldIndex) {
    Schema.Field field = schema.getField(fieldIndex);
    return toRelDataType(dataTypeFactory, field.getType());
  }
}
