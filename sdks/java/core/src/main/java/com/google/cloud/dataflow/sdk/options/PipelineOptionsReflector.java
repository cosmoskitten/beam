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
package com.google.cloud.dataflow.sdk.options;

import com.google.cloud.dataflow.sdk.util.common.ReflectHelpers;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import java.beans.Introspector;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Utilities to reflect over {@link PipelineOptions}.
 */
class PipelineOptionsReflector {
  private PipelineOptionsReflector() {}

  /**
   * Retrieve the full set of pipeline option properties visible within the type hierarchy closure
   * for the set of input interfaces. An option is "visible" if:
   *
   * <ul>
   *   <li>The option is defined within the interface hierarchy closure of the input
   *   {@link PipelineOptions}.</li>
   *   <li>The defining interface is not marked {@link Hidden}.</li>
   * </ul>
   */
  static Set<Property> collectVisibleProperties(
      Class<? extends PipelineOptions> optionsInterface) {
    Iterable<Method> methods = ReflectHelpers.getClosureOfMethodsOnInterface(optionsInterface);
    Multimap<String, Method> propsToGetters = getPropertyNamesToGetters(methods);

    ImmutableSet.Builder<Property> setBuilder = ImmutableSet.builder();
    for (Map.Entry<String, Method> propAndGetter : propsToGetters.entries()) {
      String prop = propAndGetter.getKey();
      Method getter = propAndGetter.getValue();
      Class<?> declaringClass = getter.getDeclaringClass();

      if (!PipelineOptions.class.isAssignableFrom(declaringClass)) {
        continue;
      }

      if (declaringClass.isAnnotationPresent(Hidden.class)) {
        continue;
      }

      setBuilder.add(Property.of((Class<? extends PipelineOptions>) declaringClass, prop, getter));
    }

    return setBuilder.build();
  }

  /**
   * Retrieve the full set of pipeline option properties visible within the type hierarchy of the
   * input interfaces. An option is "visible" if:
   *
   * <ul>
   *   <li>The option is defined within the interface hierarchy closure of the input
   *   {@link PipelineOptions}.</li>
   *   <li>The defining interface is not marked {@link Hidden}.</li>
   * </ul>
   */
  static Set<Property> collectVisibleProperties(
      Collection<Class<? extends PipelineOptions>> optionsInterfaces) {
    ImmutableSet.Builder<Property> setBulder = ImmutableSet.builder();
    for (Class<? extends PipelineOptions> optionsInterface : optionsInterfaces) {
      setBulder.addAll(collectVisibleProperties(optionsInterface));
    }

    return setBulder.build();
  }

  /**
   * Extra properties and their respective getter methods from a series of {@link Method methods}.
   */
  static Multimap<String, Method> getPropertyNamesToGetters(Iterable<Method> methods) {
    Multimap<String, Method> propertyNamesToGetters = HashMultimap.create();
    for (Method method : methods) {
      String methodName = method.getName();
      if ((!methodName.startsWith("get")
          && !methodName.startsWith("is"))
          || method.getParameterTypes().length != 0
          || method.getReturnType() == void.class) {
        continue;
      }
      String propertyName = Introspector.decapitalize(
          methodName.startsWith("is") ? methodName.substring(2) : methodName.substring(3));
      propertyNamesToGetters.put(propertyName, method);
    }
    return propertyNamesToGetters;
  }

  /**
   * A property defined in a {@link PipelineOptions} interface.
   */
  static class Property {
    private final Class<? extends PipelineOptions> clazz;
    private final String name;
    private final Method getter;

    static Property of(Class<? extends PipelineOptions> clazz, String name, Method getter) {
      return new Property(clazz, name, getter);
    }

    private Property(Class<? extends PipelineOptions> clazz, String name, Method getter) {
      this.clazz = clazz;
      this.name = name;
      this.getter = getter;
    }

    /**
     * The {@link PipelineOptions} interface which defines this {@link Property}.
     */
    Class<? extends PipelineOptions> definingInterface() {
      return clazz;
    }

    /**
     * Name of the property.
     */
    String name() {
      return name;
    }

    /**
     * The getter method for this property.
     */
    Method getterMethod() {
      return getter;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("definingInterface", definingInterface())
          .add("name", name())
          .add("getterMethod", getterMethod())
          .toString();
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(definingInterface(), name(), getterMethod());
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Property)) {
        return false;
      }

      Property that = (Property) obj;
      return Objects.equal(this.definingInterface(), that.definingInterface())
          && Objects.equal(this.name(), that.name())
          && Objects.equal(this.getterMethod(), that.getterMethod());
    }
  }
}
