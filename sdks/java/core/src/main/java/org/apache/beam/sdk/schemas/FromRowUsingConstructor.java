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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.utils.POJOUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithGetters;

public class FromRowUsingConstructor<T> implements FromRow<T> {
  private final Class<T> clazz;

  public FromRowUsingConstructor(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public T from(Row row) {
    return fromRow(row, clazz);
  }

  @SuppressWarnings("unchecked")
  public <S> S fromRow(Row row, Class<S> clazz) {
    if (row instanceof RowWithGetters) {
      // Efficient path: simply extract the underlying object instead of creating a new one.
      return (S) ((RowWithGetters) row).getGetterTarget();
    }

    Object[] params = new Object[row.getFieldCount()];
    Schema schema = row.getSchema();
    for (int i = 0; i < row.getFieldCount(); ++i) {
      FieldType type = schema.getField(i).getType();
      params[i] = fromValue(
          type,
          row.getValue(i),
          setter.type(),
          setter.elementType(),
          setter.mapKeyType(),
          setter.mapValueType());
    }

    try {
    Constructor<? extends S> constructor = POJOUtils.getConstructor(clazz, schema);
    return constructor.newInstance(params);
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Could not instantiate object " + clazz , e);
    }
  }

  @SuppressWarnings("unchecked")
  @Nullable
  private <S> S fromValue(
      FieldType type,
      S value,
      Type fieldType,
      Type elemenentType,
      Type keyType,
      Type valueType) {
    if (value == null) {
      return null;
    }
    if (TypeName.ROW.equals(type.getTypeName())) {
      return (S) fromRow((Row) value, (Class) fieldType);
    } else if (TypeName.ARRAY.equals(type.getTypeName())) {
      return (S)
          fromListValue(
              type.getCollectionElementType(), (List) value, elemenentType);
    } else if (TypeName.MAP.equals(type.getTypeName())) {
      return (S)
          fromMapValue(
              type.getMapKeyType(),
              type.getMapValueType(),
              (Map) value,
              keyType,
              valueType);
    } else {
      return value;
    }
  }

  @SuppressWarnings("unchecked")
  private <S> List fromListValue(
      FieldType elementType,
      List<S> rowList,
      Type elementClass) {
    List list = Lists.newArrayList();
    for (S element : rowList) {
      list.add(fromValue(elementType, element, elementClass, null, null, null));
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  private Map<?, ?> fromMapValue(
      FieldType keyType,
      FieldType valueType,
      Map<?, ?> map,
      Type keyClass,
      Type valueClass) {
    Map newMap = Maps.newHashMap();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      Object key = fromValue(keyType, entry.getKey(), keyClass, null, null, null);
      Object value =
          fromValue(valueType, entry.getValue(), valueClass, null, null, null);
      newMap.put(key, value);
    }
    return newMap;
  }
}
