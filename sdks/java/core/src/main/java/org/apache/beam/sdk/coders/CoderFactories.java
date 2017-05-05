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
package org.apache.beam.sdk.coders;

import com.google.common.base.MoreObjects;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Static utility methods for creating and working with {@link Coder}s.
 */
public final class CoderFactories {
  private CoderFactories() { } // Static utility class

  /**
   * Creates a {@link CoderFactory} for a given raw type which is able to build a coder from a
   * particular static method of a class.
   *
   * <p>The class must have the following static method:
   * <ul>
   * <li> {@code
   * public static Coder<T> of(Coder<X> argCoder1, Coder<Y> argCoder2, ...)
   * }
   * </ul>
   *
   * <p>The {@code of(...)} method will be used to construct a
   * {@code Coder<T>} from component {@link Coder}s.
   * It must accept one {@link Coder} argument for each
   * generic type parameter of {@code T}. If {@code T} takes no generic
   * type parameters, then the {@code of()} factory method should take
   * no arguments.
   *
   * <p>Note that the class {@code T} to be coded may be a
   * not-yet-specialized generic class.
   * For a generic class {@code MyClass<X>} and an actual type parameter
   * {@code Foo}, the {@link CoderFactoryFromStaticMethods} will
   * accept any {@code Coder<Foo>} and produce a {@code Coder<MyClass<Foo>>}.
   *
   * <p>For example, the {@link CoderFactory} returned by
   * {@code fromStaticMethods(ListCoder.class)}
   * will produce a {@code Coder<List<X>>} for any {@code Coder Coder<X>}.
   */
  public static CoderFactory fromStaticMethods(Class<?> rawType, Class<?> coderClazz) {
    return new CoderFactoryFromStaticMethods(rawType, coderClazz);
  }

  /**
   * Creates a {@link CoderFactory} that always returns the
   * given coder.
   */
  public static CoderFactory forCoder(TypeDescriptor<?> type, Coder<?> coder) {
    return new CoderFactoryForCoder(type, coder);
  }

  /**
   * See {@link #fromStaticMethods} for a detailed description
   * of the characteristics of this {@link CoderFactory}.
   */
  private static class CoderFactoryFromStaticMethods implements CoderFactory {

    @Override
    public <T> Coder<T> create(TypeDescriptor<T> type, List<? extends Coder<?>> componentCoders)
        throws CannotProvideCoderException {
      if (!this.rawType.equals(type.getRawType())) {
        throw new CannotProvideCoderException(String.format(
            "Unable to provide coder for %s, this factory can only provide coders for %s",
            type,
            this.rawType));
      }
      try {
        return (Coder) factoryMethod.invoke(
            null /* static */, componentCoders.toArray());
      } catch (IllegalAccessException
           | IllegalArgumentException
           | InvocationTargetException
           | NullPointerException
           | ExceptionInInitializerError exn) {
        throw new IllegalStateException(
            "error when invoking Coder factory method " + factoryMethod,
            exn);
      }
    }

    ////////////////////////////////////////////////////////////////////////////////

    // Type raw type used to filter the incoming type on.
    private final Class<?> rawType;

    // Method to create a coder given component coders
    // For a Coder class of kind * -> * -> ... n times ... -> *
    // this has type Coder<?> -> Coder<?> -> ... n times ... -> Coder<T>
    private final Method factoryMethod;

    /**
     * Returns a CoderFactory that invokes the given static factory method
     * to create the Coder.
     */
    private CoderFactoryFromStaticMethods(Class<?> rawType, Class<?> coderClazz) {
      this.rawType = rawType;
      this.factoryMethod = getFactoryMethod(coderClazz);
    }

    /**
     * Returns the static {@code of} constructor method on {@code coderClazz}
     * if it exists. It is assumed to have one {@link Coder} parameter for
     * each type parameter of {@code coderClazz}.
     */
    private Method getFactoryMethod(Class<?> coderClazz) {
      Method factoryMethodCandidate;

      // Find the static factory method of coderClazz named 'of' with
      // the appropriate number of type parameters.
      int numTypeParameters = coderClazz.getTypeParameters().length;
      Class<?>[] factoryMethodArgTypes = new Class<?>[numTypeParameters];
      Arrays.fill(factoryMethodArgTypes, Coder.class);
      try {
        factoryMethodCandidate =
            coderClazz.getDeclaredMethod("of", factoryMethodArgTypes);
      } catch (NoSuchMethodException | SecurityException exn) {
        throw new IllegalArgumentException(
            "cannot register Coder " + coderClazz + ": "
            + "does not have an accessible method named 'of' with "
            + numTypeParameters + " arguments of Coder type",
            exn);
      }
      if (!Modifier.isStatic(factoryMethodCandidate.getModifiers())) {
        throw new IllegalArgumentException(
            "cannot register Coder " + coderClazz + ": "
            + "method named 'of' with " + numTypeParameters
            + " arguments of Coder type is not static");
      }
      if (!coderClazz.isAssignableFrom(factoryMethodCandidate.getReturnType())) {
        throw new IllegalArgumentException(
            "cannot register Coder " + coderClazz + ": "
            + "method named 'of' with " + numTypeParameters
            + " arguments of Coder type does not return a " + coderClazz);
      }
      try {
        if (!factoryMethodCandidate.isAccessible()) {
          factoryMethodCandidate.setAccessible(true);
        }
      } catch (SecurityException exn) {
        throw new IllegalArgumentException(
            "cannot register Coder " + coderClazz + ": "
            + "method named 'of' with " + numTypeParameters
            + " arguments of Coder type is not accessible",
            exn);
      }

      return factoryMethodCandidate;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("rawType", rawType)
          .add("factoryMethod", factoryMethod)
          .toString();
    }
  }

  /**
   * See {@link #forCoder} for a detailed description of this {@link CoderFactory}.
   */
  private static class CoderFactoryForCoder implements CoderFactory {
    private final Coder<?> coder;
    private final TypeDescriptor<?> type;

    public CoderFactoryForCoder(TypeDescriptor<?> type, Coder<?> coder){
      this.type = type;
      this.coder = coder;
    }

    @Override
    public <T> Coder<T> create(TypeDescriptor<T> type, List<? extends Coder<?>> componentCoders)
        throws CannotProvideCoderException {
      if (!this.type.equals(type)) {
        throw new CannotProvideCoderException(String.format(
            "Unable to provide coder for %s, this factory can only provide coders for %s",
            type,
            this.type));
      }
      return (Coder) coder;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("type", type)
          .add("coder", coder)
          .toString();
    }
  }
}
