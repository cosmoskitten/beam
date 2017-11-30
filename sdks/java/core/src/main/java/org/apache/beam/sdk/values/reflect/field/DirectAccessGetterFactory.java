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

package org.apache.beam.sdk.values.reflect.field;

import static org.apache.beam.sdk.values.reflect.field.ByteBuddyUtils.implementNameGetter;
import static org.apache.beam.sdk.values.reflect.field.ByteBuddyUtils.implementTypeGetter;
import static org.apache.beam.sdk.values.reflect.field.ByteBuddyUtils.implementValueGetter;
import static org.apache.beam.sdk.values.reflect.field.ByteBuddyUtils.makeNewGetterInstance;
import static org.apache.beam.sdk.values.reflect.field.ByteBuddyUtils.subclassGetterInterface;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.Implementation;

/**
 * Implements and creates an instance of the {@link FieldValueGetter} for each public field of the
 * pojo class.
 *
 * <p>Generated {@link FieldValueGetter#get(Object)} accesses the corresponding pojo field d.
 *
 * <p>For example if the pojo looks like
 * <pre>{@code
 * public class PojoClass {
 *   public String pojoNameField;
 * }
 * }</pre>
 *
 * <p>Then, class name aside, generated {@link FieldValueGetter} will look like:
 * <pre>{@code
 * public class FieldValueGetterGenerated implements FieldValueGetter<PojoType> {
 *   public String name() {
 *     return "pojoNameField";
 *   }
 *
 *   public Class type() {
 *     return String.class;
 *   }
 *
 *   public get(PojoType pojo) {
 *     return pojo.pojoNameField;
 *   }
 * }
 * }</pre>
 *
 * <p>ByteBuddy is used to generate the code. Class naming is left to ByteBuddy's defaults.
 *
 * <p>Class is injected into ByteBuddyUtils.class.getClassLoader().
 * See {@link ByteBuddyUtils#makeNewGetterInstance(String, DynamicType.Builder)}
 * and ByteBuddy documentation for details.
 */
public class DirectAccessGetterFactory {

  /**
   * Returns the list of the getters, one for each public field of the pojoClass.
   */
  public static List<FieldValueGetter> generateGetters(
      ByteBuddy byteBuddy, Class pojoClass) {

    ImmutableList.Builder<FieldValueGetter> getters = ImmutableList.builder();

    List<Field> publicFields = getPublicFields(pojoClass);

    for (Field field : publicFields) {
      getters.add(createFieldValueGetter(byteBuddy, pojoClass, field));
    }

    return getters.build();
  }

  private static FieldValueGetter createFieldValueGetter(
      ByteBuddy byteBuddy,
      Class clazz,
      Field field) {

    DynamicType.Builder<FieldValueGetter> getterBuilder = subclassGetterInterface(byteBuddy, clazz);

    getterBuilder = implementNameGetter(getterBuilder, field.getName());
    getterBuilder = implementTypeGetter(getterBuilder, field.getType());
    getterBuilder = implementValueGetter(getterBuilder, accessorImplementation(field));

    return makeNewGetterInstance(field.getName(), getterBuilder);
  }

  private static Implementation accessorImplementation(Field field) {
    return new DirectAccessImplementation(field);
  }

  private static List<Field> getPublicFields(Class clazz) {
    ImmutableList.Builder<Field> builder = ImmutableList.builder();

    for (Field field : clazz.getFields()) {
      if (Modifier.isPublic(field.getModifiers())) {
        builder.add(field);
      }
    }

    return builder.build();
  }
}
