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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.common.ReflectHelpers;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Parses a {@link DoFn} and computes its {@link DoFnSignature}. See {@link #getSignature}.
 */
public abstract class DoFnReflector {
  private DoFnReflector() {}

  private static final Map<Class<?>, DoFnSignature> SIGNATURE_CACHE = new LinkedHashMap<>();

  /** @return the {@link DoFnReflector} for the given {@link DoFn}. */
  static synchronized DoFnSignature getSignature(
      @SuppressWarnings("rawtypes") Class<? extends DoFn> fn) {
    DoFnSignature signature = SIGNATURE_CACHE.get(fn);
    if (signature != null) {
      return signature;
    }

    signature = getSignatureImpl(fn);
    SIGNATURE_CACHE.put(fn, signature);
    return signature;
  }

  /** Analyzes a given {@link DoFn} class and extracts its {@link DoFnSignature}. */
  private static DoFnSignature getSignatureImpl(Class<? extends DoFn> fnClass) {
    TypeToken<?> inputT = null;
    TypeToken<?> outputT = null;

    // Extract the input and output type.
    if (!DoFn.class.isAssignableFrom(fnClass)) {
      throw new IllegalArgumentException(
          String.format("%s must be subtype of DoFn", fnClass.getSimpleName()));
    }
    for (TypeToken<?> supertype : TypeToken.of(fnClass).getTypes()) {
      if (!supertype.getRawType().equals(DoFn.class)) {
        continue;
      }
      Type[] args = ((ParameterizedType) supertype.getType()).getActualTypeArguments();
      inputT = TypeToken.of(args[0]);
      outputT = TypeToken.of(args[1]);
    }
    assert inputT != null;

    Method processElementMethod = findAnnotatedMethod(DoFn.ProcessElement.class, fnClass, true);
    Method startBundleMethod = findAnnotatedMethod(DoFn.StartBundle.class, fnClass, false);
    Method finishBundleMethod = findAnnotatedMethod(DoFn.FinishBundle.class, fnClass, false);

    return new DoFnSignature(
        inputT,
        outputT,
        analyzeProcessElementMethod(processElementMethod, inputT, outputT),
        (startBundleMethod == null)
            ? null
            : analyzeBundleMethod(startBundleMethod, inputT, outputT),
        (finishBundleMethod == null)
            ? null
            : analyzeBundleMethod(finishBundleMethod, inputT, outputT));
  }

  private static <InputT, OutputT>
      TypeToken<DoFn<InputT, OutputT>.ProcessContext> doFnProcessContextTypeOf(
          TypeToken<InputT> inputT, TypeToken<OutputT> outputT) {
    return new TypeToken<DoFn<InputT, OutputT>.ProcessContext>() {}.where(
            new TypeParameter<InputT>() {}, inputT)
        .where(new TypeParameter<OutputT>() {}, outputT);
  }

  private static <InputT, OutputT> TypeToken<DoFn<InputT, OutputT>.Context> doFnContextTypeOf(
      TypeToken<InputT> inputT, TypeToken<OutputT> outputT) {
    return new TypeToken<DoFn<InputT, OutputT>.Context>() {}.where(
            new TypeParameter<InputT>() {}, inputT)
        .where(new TypeParameter<OutputT>() {}, outputT);
  }

  private static <InputT> TypeToken<DoFn.InputProvider<InputT>> inputProviderTypeOf(
      TypeToken<InputT> inputT) {
    return new TypeToken<DoFn.InputProvider<InputT>>() {}.where(
        new TypeParameter<InputT>() {}, inputT);
  }

  private static <OutputT> TypeToken<DoFn.OutputReceiver<OutputT>> outputReceiverTypeOf(
      TypeToken<OutputT> inputT) {
    return new TypeToken<DoFn.OutputReceiver<OutputT>>() {}.where(
        new TypeParameter<OutputT>() {}, inputT);
  }

  static DoFnSignature.ProcessElementMethod analyzeProcessElementMethod(
      Method m, TypeToken<?> inputT, TypeToken<?> outputT) {
    if (!void.class.equals(m.getReturnType())) {
      throw new IllegalArgumentException(
          String.format("%s must have a void return type", format(m)));
    }
    if (m.isVarArgs()) {
      throw new IllegalArgumentException(String.format("%s must not have var args", format(m)));
    }

    TypeToken<?> processContextToken = doFnProcessContextTypeOf(inputT, outputT);

    Type[] params = m.getGenericParameterTypes();
    TypeToken<?> contextToken = null;
    if (params.length > 0) {
      contextToken = TypeToken.of(params[0]);
    }
    if (contextToken == null
        || !contextToken.getRawType().equals(processContextToken.getRawType())) {
      throw new IllegalArgumentException(
          String.format(
              "%s must take a %s as its first argument",
              format(m), processContextToken.getRawType().getSimpleName()));
    }

    List<DoFnSignature.ProcessElementMethod.Parameter> extraParameters = new ArrayList<>();
    TypeToken<?> expectedInputProviderT = inputProviderTypeOf(inputT);
    TypeToken<?> expectedOutputReceiverT = outputReceiverTypeOf(outputT);
    for (int i = 1; i < params.length; ++i) {
      TypeToken<?> param = TypeToken.of(params[i]);
      if (param.isSubtypeOf(BoundedWindow.class)) {
        if (extraParameters.contains(DoFnSignature.ProcessElementMethod.Parameter.BOUNDED_WINDOW)) {
          throw new IllegalArgumentException("Multiple BoundedWindow parameters");
        }
        extraParameters.add(DoFnSignature.ProcessElementMethod.Parameter.BOUNDED_WINDOW);
      } else if (param.isSubtypeOf(DoFn.InputProvider.class)) {
        if (extraParameters.contains(DoFnSignature.ProcessElementMethod.Parameter.INPUT_PROVIDER)) {
          throw new IllegalArgumentException("Multiple InputProvider parameters");
        }
        if (!param.isSupertypeOf(expectedInputProviderT)) {
          throw new IllegalArgumentException(
              String.format(
                  "Wrong type of InputProvider parameter for method %s: %s, should be %s",
                  format(m), formatType(param), formatType(expectedInputProviderT)));
        }
        extraParameters.add(DoFnSignature.ProcessElementMethod.Parameter.INPUT_PROVIDER);
      } else if (param.isSubtypeOf(DoFn.OutputReceiver.class)) {
        if (extraParameters.contains(
            DoFnSignature.ProcessElementMethod.Parameter.OUTPUT_RECEIVER)) {
          throw new IllegalArgumentException("Multiple OutputReceiver parameters");
        }
        if (!param.isSupertypeOf(expectedOutputReceiverT)) {
          throw new IllegalArgumentException(
              String.format(
                  "Wrong type of OutputReceiver parameter for method %s: %s, should be %s",
                  format(m), formatType(param), formatType(expectedOutputReceiverT)));
        }
        extraParameters.add(DoFnSignature.ProcessElementMethod.Parameter.OUTPUT_RECEIVER);
      } else {
        List<String> allowedParamTypes =
            Arrays.asList(formatType(new TypeToken<BoundedWindow>() {}));
        throw new IllegalArgumentException(
            String.format(
                "%s is not a valid context parameter for method %s. Should be one of %s",
                formatType(param), format(m), allowedParamTypes));
      }
    }

    return new DoFnSignature.ProcessElementMethod(m, extraParameters);
  }

  static DoFnSignature.BundleMethod analyzeBundleMethod(
      Method m, TypeToken<?> inputT, TypeToken<?> outputT) {
    if (!void.class.equals(m.getReturnType())) {
      throw new IllegalArgumentException(
          String.format("%s must have a void return type", format(m)));
    }
    if (m.isVarArgs()) {
      throw new IllegalArgumentException(String.format("%s must not have var args", format(m)));
    }

    TypeToken<?> expectedContextToken = doFnContextTypeOf(inputT, outputT);

    Type[] params = m.getGenericParameterTypes();
    if (params.length != 1) {
      throw new IllegalArgumentException(
          String.format(
              "%s must have a single argument of type %s",
              format(m), formatType(expectedContextToken)));
    }
    TypeToken<?> contextToken = TypeToken.of(params[0]);
    if (!contextToken.getRawType().equals(expectedContextToken.getRawType())) {
      throw new IllegalArgumentException(
          String.format(
              "Wrong type of context argument to %s: %s, must be %s",
              format(m), formatType(contextToken), formatType(expectedContextToken)));
    }

    return new DoFnSignature.BundleMethod(m);
  }

  private static Collection<Method> declaredMethodsWithAnnotation(
      Class<? extends Annotation> anno, Class<?> startClass, Class<?> stopClass) {
    Collection<Method> matches = new ArrayList<>();

    Class<?> clazz = startClass;
    LinkedHashSet<Class<?>> interfaces = new LinkedHashSet<>();

    // First, find all declared methods on the startClass and parents (up to stopClass)
    while (clazz != null && !clazz.equals(stopClass)) {
      for (Method method : clazz.getDeclaredMethods()) {
        if (method.isAnnotationPresent(anno)) {
          matches.add(method);
        }
      }

      Collections.addAll(interfaces, clazz.getInterfaces());

      clazz = clazz.getSuperclass();
    }

    // Now, iterate over all the discovered interfaces
    for (Method method : ReflectHelpers.getClosureOfMethodsOnInterfaces(interfaces)) {
      if (method.isAnnotationPresent(anno)) {
        matches.add(method);
      }
    }
    return matches;
  }

  private static Method findAnnotatedMethod(
      Class<? extends Annotation> anno, Class<?> fnClazz, boolean required) {
    Collection<Method> matches = declaredMethodsWithAnnotation(anno, fnClazz, DoFn.class);

    if (matches.size() == 0) {
      if (required) {
        throw new IllegalArgumentException(
            String.format(
                "No method annotated with @%s found in %s",
                anno.getSimpleName(), fnClazz.getName()));
      } else {
        return null;
      }
    }

    // If we have at least one match, then either it should be the only match
    // or it should be an extension of the other matches (which came from parent
    // classes).
    Method first = matches.iterator().next();
    for (Method other : matches) {
      if (!first.getName().equals(other.getName())
          || !Arrays.equals(first.getParameterTypes(), other.getParameterTypes())) {
        throw new IllegalArgumentException(
            String.format(
                "Found multiple methods annotated with @%s. [%s] and [%s]",
                anno.getSimpleName(), format(first), format(other)));
      }
    }

    // We need to be able to call it. We require it is public.
    if ((first.getModifiers() & Modifier.PUBLIC) == 0) {
      throw new IllegalArgumentException(format(first) + " must be public");
    }

    // And make sure its not static.
    if ((first.getModifiers() & Modifier.STATIC) != 0) {
      throw new IllegalArgumentException(format(first) + " must not be static");
    }

    return first;
  }

  private static String format(Method m) {
    return ReflectHelpers.CLASS_AND_METHOD_FORMATTER.apply(m);
  }

  private static String formatType(TypeToken<?> t) {
    return ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(t.getType());
  }
}
