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
import java.util.List;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.values.BeamRecordType;
import org.apache.beam.sdk.values.reflect.field.FieldValueGetter;

/**
 * A default factory to create a {@link BeamRecordType} based on pojo field getters.
 */
public class DefaultRecordTypeFactory implements RecordTypeFactory {

  private static final CoderRegistry CODER_REGISTRY = CoderRegistry.createDefault();

  /**
   * Uses {@link FieldValueGetter#name()} as field names.
   * Uses {@link CoderRegistry#createDefault()} to get coders for {@link FieldValueGetter#type()}.
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
      try {
        coders.add(CODER_REGISTRY.getCoder(fieldValueGetter.type()));
      } catch (CannotProvideCoderException e) {
        throw new UnsupportedOperationException(
            "Fields of type "
                + fieldValueGetter.type().getSimpleName() + " are not supported yet", e);
      }
    }

    return coders.build();
  }
}
