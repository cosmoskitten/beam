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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithGetters;

class FromRowUsingSetters<T> implements SerializableFunction<Row, T> {
  private final Class<T> clazz;
  private final FieldValueSetterFactory fieldValueSetterFactory;
  private final FieldValueTypeInformationFactory fieldValueTypeInformationFactory;

  public FromRowUsingSetters(
      Class<T> clazz,
      FieldValueSetterFactory fieldValueSetterFactory,
      FieldValueTypeInformationFactory fieldValueTypeInformationFactory) {
    FieldValueSetterFactory cachingSetterFactory =
        new FieldValueSetterFactory() {
          @Nullable
          private volatile ConcurrentHashMap<Class, List<FieldValueSetter>> settersMap = null;

          private final FieldValueSetterFactory innerFactory = fieldValueSetterFactory;

          @Override
          public List<FieldValueSetter> createSetters(Class<?> targetClass, Schema schema) {
            if (settersMap == null) {
              settersMap = new ConcurrentHashMap<>();
            }
            List<FieldValueSetter> setters = settersMap.get(targetClass);
            if (setters != null) {
              return setters;
            }
            setters = innerFactory.createSetters(targetClass, schema);
            settersMap.put(targetClass, setters);
            return setters;
          }
        };

    this.clazz = clazz;
    this.fieldValueSetterFactory = cachingSetterFactory;
    // TODO: THIS MUST BE CACHED AS WELL
    this.fieldValueTypeInformationFactory = fieldValueTypeInformationFactory;
  }

  @Override
  public T apply(Row row) {
    return fromRow(row, clazz, fieldValueSetterFactory);
  }

  @SuppressWarnings("unchecked")
  private <S> S fromRow(Row row, Class<S> clazz, FieldValueSetterFactory setterFactory) {
    if (row instanceof RowWithGetters) {
      // Efficient path: simply extract the underlying object instead of creating a new one.
      return (S) ((RowWithGetters) row).getGetterTarget();
    }

    S object;
    try {
      object = clazz.getDeclaredConstructor().newInstance();
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | InstantiationException e) {
      throw new RuntimeException("Failed to instantiate object ", e);
    }

    Schema schema = row.getSchema();
    List<FieldValueSetter> setters = setterFactory.createSetters(clazz, schema);
    checkState(
        setters.size() == row.getFieldCount(),
        "Did not have a matching number of setters and fields.");
    List<FieldValueTypeInformation> typeInformations =
        fieldValueTypeInformationFactory.getTypeInformations(clazz, schema);
    checkState(
        typeInformations.size() == row.getFieldCount(),
        "Did not have a matching number of typeInformations and fields.");

    // Iterate over the row, and set (possibly recursively) each field in the underlying object
    // using the setter.
    for (int i = 0; i < row.getFieldCount(); ++i) {
      FieldType type = schema.getField(i).getType();
      FieldValueSetter setter = setters.get(i);
      FieldValueTypeInformation typeInformation = typeInformations.get(i);
      setter.set(
          object,
          fromValue(
              type,
              row.getValue(i),
              typeInformation.type(),
              typeInformation.elementType(),
              typeInformation.mapKeyType(),
              typeInformation.mapValueType(),
              setterFactory));
    }
    return object;
  }

  @SuppressWarnings("unchecked")
  @Nullable
  private <S> S fromValue(
      FieldType type,
      S value,
      Type fieldType,
      Type elemenentType,
      Type keyType,
      Type valueType,
      FieldValueSetterFactory setterFactory) {
    if (value == null) {
      return null;
    }
    if (TypeName.ROW.equals(type.getTypeName())) {
      return (S) fromRow((Row) value, (Class) fieldType, setterFactory);
    } else if (TypeName.ARRAY.equals(type.getTypeName())) {
      return (S)
          fromListValue(
              type.getCollectionElementType(), (List) value, elemenentType, setterFactory);
    } else if (TypeName.MAP.equals(type.getTypeName())) {
      return (S)
          fromMapValue(
              type.getMapKeyType(),
              type.getMapValueType(),
              (Map) value,
              keyType,
              valueType,
              setterFactory);
    } else {
      return value;
    }
  }

  @SuppressWarnings("unchecked")
  private <S> List fromListValue(
      FieldType elementType,
      List<S> rowList,
      Type elementClass,
      FieldValueSetterFactory setterFactory) {
    List list = Lists.newArrayList();
    for (S element : rowList) {
      list.add(fromValue(elementType, element, elementClass, null, null, null, setterFactory));
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  private Map<?, ?> fromMapValue(
      FieldType keyType,
      FieldType valueType,
      Map<?, ?> map,
      Type keyClass,
      Type valueClass,
      FieldValueSetterFactory setterFactory) {
    Map newMap = Maps.newHashMap();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      Object key = fromValue(keyType, entry.getKey(), keyClass, null, null, null, setterFactory);
      Object value =
          fromValue(valueType, entry.getValue(), valueClass, null, null, null, setterFactory);
      newMap.put(key, value);
    }
    return newMap;
  }
}
