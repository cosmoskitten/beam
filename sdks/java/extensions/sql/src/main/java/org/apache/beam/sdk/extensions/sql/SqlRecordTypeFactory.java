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

package org.apache.beam.sdk.extensions.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import org.apache.beam.sdk.values.BeamRecordType;
import org.apache.beam.sdk.values.reflect.RecordTypeFactory;
import org.apache.beam.sdk.values.reflect.field.FieldValueGetter;

/**
 * Implementation of the {@link RecordTypeFactory} to return instances of {@link BeamRecordSqlType}
 * instead of {@link BeamRecordType}.
 *
 * <p>This is currently needed because Beam SQL uses {@link java.sql.Types} to map
 * between Calcite types, Java types, and coders.
 */
public class SqlRecordTypeFactory implements RecordTypeFactory {

  static final ImmutableMap<Class, Integer> SQL_TYPES = ImmutableMap
      .<Class, Integer>builder()
      .put(Byte.class, Types.TINYINT)
      .put(Short.class, Types.SMALLINT)
      .put(Integer.class, Types.INTEGER)
      .put(Long.class, Types.BIGINT)
      .put(Float.class, Types.FLOAT)
      .put(Double.class, Types.DOUBLE)
      .put(BigDecimal.class, Types.DECIMAL)
      .put(Boolean.class, Types.BOOLEAN)
      .put(String.class, Types.VARCHAR)
      .put(GregorianCalendar.class, Types.TIME)
      .put(Date.class, Types.TIMESTAMP)
      .build();

  @Override
  public BeamRecordSqlType createRecordType(Iterable<FieldValueGetter> getters) {
    return BeamRecordSqlType.create(
        fieldNames(getters),
        sqlTypes(getters));
  }

  private List<String> fieldNames(Iterable<FieldValueGetter> getters) {
    ImmutableList.Builder<String> names = ImmutableList.builder();

    for (FieldValueGetter fieldValueGetter : getters) {
      names.add(fieldValueGetter.name());
    }

    return names.build();
  }

  private List<Integer> sqlTypes(Iterable<FieldValueGetter> getters) {
    ImmutableList.Builder<Integer> sqlTypes = ImmutableList.builder();

    for (FieldValueGetter fieldValueGetter : getters) {
      if (!SQL_TYPES.containsKey(fieldValueGetter.type())) {
        throw new UnsupportedOperationException(
            "Field type " + fieldValueGetter.type().getSimpleName() + " is not supported yet");
      }

      sqlTypes.add(SQL_TYPES.get(fieldValueGetter.type()));
    }

    return sqlTypes.build();
  }
}
