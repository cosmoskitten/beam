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
package org.apache.beam.sdk.schemas.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy.Default;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import org.apache.avro.specific.SpecificRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.ConvertType;
import org.apache.beam.sdk.schemas.utils.ReflectUtils.ClassWithSchema;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.TypeDescriptor;

class AvroByteBuddyUtils {
  private static final ByteBuddy BYTE_BUDDY = new ByteBuddy();

  // Cache the generated constructors.
  private static final Map<ClassWithSchema, Constructor> CACHED_CONSTRUCTORS =
      Maps.newConcurrentMap();

  static <T extends SpecificRecord> Constructor<T> getConstructor(Class<T> clazz, Schema schema) {
    return CACHED_CONSTRUCTORS.computeIfAbsent(
        new ClassWithSchema(clazz, schema), c -> createConstructor(clazz, schema));
  }

  private static <T> Constructor<? extends T> createConstructor(Class<T> clazz, Schema schema) {
    Constructor baseConstructor = null;
    Constructor[] constructors = clazz.getDeclaredConstructors();
    for (Constructor constructor : constructors) {
      // TODO: This assumes that Avro only generates one constructor with this many fields.
      if (constructor.getParameterCount() == schema.getFieldCount()) {
        baseConstructor = constructor;
      }
    }
    if (baseConstructor == null) {
      throw new RuntimeException("No matching constructor found for class " + clazz);
    }

    // Generate a method call to invoke the SpecificRecord's constructor. This will be invoked
    // by the subclass constructor.
    MethodCall invokeBase = MethodCall.invoke(baseConstructor);

    // The types in the AVRO-generated constructor might be the types returned by Beam's Row class.
    // Generate a subclass with the correct types for parameters, and forward the call to the
    // base class. We know that AVRO generates constructor parameters in the same order as fields
    // in the schema, so we can just add the parameters sequentially.
    ConvertType convertType = new ConvertType(true);
    List<Type> generatedConstructorParameterTypes =
        Lists.newArrayListWithExpectedSize(baseConstructor.getParameterTypes().length);
    for (int i = 0; i < baseConstructor.getParameterTypes().length; ++i) {
      // Turn the AVRO-generated type into one that Row expects, and add it as a constructor
      // parameter to the subclass being generated.
      Class<?> baseType = baseConstructor.getParameterTypes()[i];
      java.lang.reflect.Type convertedType = convertType.convert(TypeDescriptor.of(baseType));
      generatedConstructorParameterTypes.add(convertedType);

      // This will run inside the generated constructor. Read the parameter and convert it to the
      // type required by the base-class constructor.
      StackManipulation readField = MethodVariableAccess.REFERENCE.loadFrom(i + 1);
      StackManipulation convertedField =
          new ByteBuddyUtils.ConvertValueForSetter(readField).convert(TypeDescriptor.of(baseType));
      invokeBase = invokeBase.with(convertedField, baseType);
    }

    try {
      DynamicType.Builder<? extends T> builder =
          BYTE_BUDDY
              .subclass(clazz, Default.NO_CONSTRUCTORS)
              .defineConstructor(Visibility.PUBLIC)
              .withParameters(generatedConstructorParameterTypes)
              .intercept(invokeBase);

      // Generate the class and return a reference to the constructor.
      Class typeArray[] =
          generatedConstructorParameterTypes.toArray(
              new Class[generatedConstructorParameterTypes.size()]);
      return builder
          .make()
          .load(ReflectHelpers.findClassLoader(), ClassLoadingStrategy.Default.INJECTION)
          .getLoaded()
          .getDeclaredConstructor(typeArray);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(
          "Unable to generate a getter for class " + clazz + " with schema " + schema);
    }
  }
}
