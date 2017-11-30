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

package org.apache.beam.sdk.values.reflect;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.bytebuddy.ByteBuddy;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.BeamRecordType;
import org.apache.beam.sdk.values.reflect.field.DirectAccessGetterFactory;
import org.apache.beam.sdk.values.reflect.field.FieldValueGetter;
import org.apache.beam.sdk.values.reflect.field.GetterMethodGetterFactory;

/**
 * Generates the code to create BeamRecordTypes and BeamRecords based on pojos.
 *
 * <p>Generated record types are cached in the instance of this factory.
 *
 * <p>Currently only 1-1 mapping is supported between pojo classes and record types.
 */
public class RecordFactory {
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();

  private RecordTypeFactory recordTypeFactory;
  private final Map<Class, RecordTypeGetters> recordTypesCache = new HashMap<>();

  /**
   * Create new instance based on default record type factory.
   *
   * <p>Use this to create instances of {@link BeamRecordType}.
   */
  public RecordFactory() {
    this(new DefaultRecordTypeFactory());
  }

  /**
   * Create new instance with custom record type factory.
   *
   * <p>For example this can be used to create BeamRecordSqlTypes instead of BeamRecordType.
   */
  public RecordFactory(RecordTypeFactory recordTypeFactory) {
    this.recordTypeFactory = recordTypeFactory;
  }

  /**
   * Create a BeamRecord of the pojo.
   *
   * <p>This implementation copies the values of the pojo fields into the record fields on creation.
   *
   * <p>Currently all public fields and getters are used to populate the record type and instance.
   *
   * <p>Field names for getters are stripped of the 'get' prefix.
   * For example record field 'name' will be generated for 'getName()' pojo method.
   */
  public BeamRecord newRecordCopyOf(Object pojo) {
    RecordTypeGetters getters = getRowType(pojo.getClass());
    List<Object> fieldValues = getFieldValues(getters.valueGetters(), pojo);
    return new BeamRecord(getters.recordType(), fieldValues);
  }

  private synchronized RecordTypeGetters getRowType(Class pojoClass) {
    if (recordTypesCache.containsKey(pojoClass)) {
      return recordTypesCache.get(pojoClass);
    }

    List<FieldValueGetter> fieldValueGetters = createGetters(pojoClass);
    BeamRecordType rowType = recordTypeFactory.createRecordType(fieldValueGetters);
    recordTypesCache.put(pojoClass, new RecordTypeGetters(rowType, fieldValueGetters));

    return recordTypesCache.get(pojoClass);
  }

  private List<FieldValueGetter> createGetters(Class pojoClass) {
    return ImmutableList.
        <FieldValueGetter>builder()
        .addAll(DirectAccessGetterFactory.generateGetters(BYTE_BUDDY, pojoClass))
        .addAll(GetterMethodGetterFactory.generateGetters(BYTE_BUDDY, pojoClass))
        .build();
  }

  private List<Object> getFieldValues(List<FieldValueGetter> fieldValueGetters, Object pojo) {
    ImmutableList.Builder<Object> builder = ImmutableList.builder();

    for (FieldValueGetter fieldValueGetter : fieldValueGetters) {
      builder.add(fieldValueGetter.get(pojo));
    }

    return builder.build();
  }
}
