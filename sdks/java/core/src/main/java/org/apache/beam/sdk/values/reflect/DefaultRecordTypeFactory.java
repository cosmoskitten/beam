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
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.List;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.values.BeamRecordType;
import org.apache.beam.sdk.values.reflect.field.FieldValueGetter;

/**
 * A default factory to create a {@link BeamRecordType} based on pojo field getters.
 */
public class DefaultRecordTypeFactory implements RecordTypeFactory {

  private static final ImmutableMap<Class, Coder> JAVA_TYPES_CODERS = ImmutableMap
      .<Class, Coder>builder()
      .put(Byte.class, ByteCoder.of())
      .put(Integer.class, VarIntCoder.of())
      .put(Long.class, VarLongCoder.of())
      .put(Double.class, DoubleCoder.of())
      .put(BigDecimal.class, BigDecimalCoder.of())
      .put(Boolean.class, BooleanCoder.of())
      .put(String.class, StringUtf8Coder.of())
      .build();

  /**
   * Takes field names and types from the getters, maps them to coders, returns
   * an instance of the {@link BeamRecordType}.
   *
   * <p>Coders not for all field types are implemented.
   * Supported field types at the moment are:
   *    Byte, Integer, Long, Double, BigDecimal, Boolean, String.
   */
  @Override
  public BeamRecordType createRecordType(Iterable<FieldValueGetter> fieldValueGetters) {
    return new BeamRecordType(
        getFieldNames(fieldValueGetters),
        getFieldCoders(fieldValueGetters));
  }

  private static List<String> getFieldNames(Iterable<FieldValueGetter> fieldValueGetters) {
    ImmutableList.Builder<String> names = ImmutableList.builder();

    for (FieldValueGetter fieldValueGetter : fieldValueGetters) {
      names.add(fieldValueGetter.name());
    }

    return names.build();
  }

  private static List<Coder> getFieldCoders(Iterable<FieldValueGetter> fieldValueGetters) {
    ImmutableList.Builder<Coder> coders = ImmutableList.builder();

    for (FieldValueGetter fieldValueGetter : fieldValueGetters) {
      if (!JAVA_TYPES_CODERS.containsKey(fieldValueGetter.type())) {
        throw new UnsupportedOperationException(
            "Fields if type " + fieldValueGetter.type().getSimpleName() + " are not supported yet");
      }

      coders.add(JAVA_TYPES_CODERS.get(fieldValueGetter.type()));
    }

    return coders.build();
  }
}
