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

/** Function to convert a {@link Row} to a user type using a creator factory. */
class FromRowUsingConstructor<T> implements SerializableFunction<Row, T> {
  private final Class<T> clazz;
  private final SchemaTypeCreatorFactory schemaTypeCreatorFactory;

  private final FieldValueTypeInformationFactory fieldValueTypeInformationFactory;

  public FromRowUsingConstructor(
      Class<T> clazz,
      SchemaTypeCreatorFactory schemaTypeCreatorFactory,
      FieldValueTypeInformationFactory fieldValueTypeInformationFactory) {
    FieldValueTypeInformationFactory cachingTypeFactory =
        new FieldValueTypeInformationFactory() {
          @Nullable
          private volatile ConcurrentHashMap<Class, List<FieldValueTypeInformation>> settersMap =
              null;

          private final FieldValueTypeInformationFactory innerFactory =
              fieldValueTypeInformationFactory;

          @Override
          public List<FieldValueTypeInformation> getTypeInformations(
              Class<?> targetClass, Schema schema) {
            if (settersMap == null) {
              settersMap = new ConcurrentHashMap<>();
            }
            List<FieldValueTypeInformation> setters = settersMap.get(targetClass);
            if (setters != null) {
              return setters;
            }
            setters = innerFactory.getTypeInformations(targetClass, schema);
            settersMap.put(targetClass, setters);
            return setters;
          }
        };

    this.clazz = clazz;
    this.schemaTypeCreatorFactory = schemaTypeCreatorFactory;
    this.fieldValueTypeInformationFactory = cachingTypeFactory;
  }

  @Override
  public T apply(Row row) {
    return fromRow(row, clazz, fieldValueTypeInformationFactory);
  }

  @SuppressWarnings("unchecked")
  public <S> S fromRow(Row row, Class<S> clazz, FieldValueTypeInformationFactory typeFactory) {
    if (row instanceof RowWithGetters) {
      // Efficient path: simply extract the underlying object instead of creating a new one.
      return (S) ((RowWithGetters) row).getGetterTarget();
    }

    Object[] params = new Object[row.getFieldCount()];
    Schema schema = row.getSchema();
    List<FieldValueTypeInformation> typeInformations =
        typeFactory.getTypeInformations(clazz, schema);
    checkState(
        typeInformations.size() == row.getFieldCount(),
        "Did not have a matching number of type informations and fields.");

    for (int i = 0; i < row.getFieldCount(); ++i) {
      FieldType type = schema.getField(i).getType();
      FieldValueTypeInformation typeInformation = typeInformations.get(i);
      params[i] =
          fromValue(
              type,
              row.getValue(i),
              typeInformation.type(),
              typeInformation.elementType(),
              typeInformation.mapKeyType(),
              typeInformation.mapValueType(),
              typeFactory);
    }

    SchemaTypeCreator<S> creator = schemaTypeCreatorFactory.getCreator(clazz, schema);
    return creator.create(params);
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
      FieldValueTypeInformationFactory typeFactory) {
    if (value == null) {
      return null;
    }
    if (TypeName.ROW.equals(type.getTypeName())) {
      return (S) fromRow((Row) value, (Class) fieldType, typeFactory);
    } else if (TypeName.ARRAY.equals(type.getTypeName())) {
      return (S)
          fromListValue(type.getCollectionElementType(), (List) value, elemenentType, typeFactory);
    } else if (TypeName.MAP.equals(type.getTypeName())) {
      return (S)
          fromMapValue(
              type.getMapKeyType(),
              type.getMapValueType(),
              (Map) value,
              keyType,
              valueType,
              typeFactory);
    } else {
      return value;
    }
  }

  @SuppressWarnings("unchecked")
  private <S> List fromListValue(
      FieldType elementType,
      List<S> rowList,
      Type elementClass,
      FieldValueTypeInformationFactory typeFactory) {
    List list = Lists.newArrayList();
    for (S element : rowList) {
      list.add(fromValue(elementType, element, elementClass, null, null, null, typeFactory));
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
      FieldValueTypeInformationFactory typeFactory) {
    Map newMap = Maps.newHashMap();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      Object key = fromValue(keyType, entry.getKey(), keyClass, null, null, null, typeFactory);
      Object value =
          fromValue(valueType, entry.getValue(), valueClass, null, null, null, typeFactory);
      newMap.put(key, value);
    }
    return newMap;
  }
}
