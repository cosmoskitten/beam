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
package org.apache.beam.sdk.transforms.reflect;

import com.google.common.base.CharMatcher;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.NamingStrategy;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers.DoFnMethodDelegation;

/**
 * Dynamically generates {@link OnTimerInvoker} instances for invoking a particular {@link
 * TimerId} on a particular {@link DoFn}.
 */
class OnTimerInvokers {
  public static final OnTimerInvokers INSTANCE = new OnTimerInvokers();

  /**
   * The field name for the delegate of {@link DoFn} subclass that a bytebuddy invoker will call.
   */
  private static final String FN_DELEGATE_FIELD_NAME = "delegate";

  /**
   * A cache of constructors of generated {@link OnTimerInvoker} classes, keyed by {@link DoFn}
   * class and then by {@link TimerId}.
   *
   * <p>Needed because generating an invoker class is expensive, and to avoid generating an
   * excessive number of classes consuming PermGen memory in Java's that still have PermGen.
   */
  private final Map<Class<?>, Map<String, Constructor<?>>> constructorCache = new LinkedHashMap<>();

  private OnTimerInvokers() {}

  /** Creates invoker. */
  public <InputT, OutputT> OnTimerInvoker<InputT, OutputT> invokerForTimer(
      DoFn<InputT, OutputT> fn, String timerId) {

    DoFnSignature signature = DoFnSignatures.INSTANCE.getSignature(fn.getClass());

    try {
      @SuppressWarnings("unchecked")
      OnTimerInvoker<InputT, OutputT> invoker =
          (OnTimerInvoker<InputT, OutputT>)
              getByteBuddyOnTimerInvokerConstructor(signature, timerId).newInstance(fn);
      return invoker;
    } catch (InstantiationException
        | IllegalAccessException
        | IllegalArgumentException
        | InvocationTargetException
        | SecurityException e) {
      throw new RuntimeException(
          String.format(
              "Unable to bind @%s invoker for %s",
              DoFn.OnTimer.class.getSimpleName(), fn.getClass().getName()),
          e);
    }
  }

  private void putConstructor(
      Class<? extends DoFn<?, ?>> fnClass, String timerId, Constructor<?> constructor) {
    Map<String, Constructor<?>> invokerConstructors = constructorCache.get(fnClass);
    if (invokerConstructors == null) {
      invokerConstructors = new HashMap<>();
      constructorCache.put(fnClass, invokerConstructors);
    }
    invokerConstructors.put(timerId, constructor);
  }

  /** Returns the constructor for a generated invoker for the requested timer. */
  private synchronized Constructor<?> getByteBuddyOnTimerInvokerConstructor(
      DoFnSignature signature, String timerId) {

    Class<? extends DoFn<?, ?>> fnClass = signature.fnClass();
    Map<String, Constructor<?>> invokerConstructors = constructorCache.get(fnClass);
    Constructor<?> constructor =
        invokerConstructors == null ? null : invokerConstructors.get(timerId);
    if (constructor == null) {
      Class<? extends OnTimerInvoker<?, ?>> invokerClass =
          generateOnTimerInvokerClass(signature, timerId);
      try {
        constructor = invokerClass.getConstructor(fnClass);
      } catch (IllegalArgumentException | NoSuchMethodException | SecurityException e) {
        throw new RuntimeException(e);
      }
      putConstructor(fnClass, timerId, constructor);
    }
    return constructor;
  }

  /**
   * Generates a {@link OnTimerInvoker} class for the given {@link DoFnSignature} and {@link
   * TimerId}.
   */
  private static Class<? extends OnTimerInvoker<?, ?>> generateOnTimerInvokerClass(
      DoFnSignature signature, String timerId) {
    Class<? extends DoFn<?, ?>> fnClass = signature.fnClass();

    final TypeDescription clazzDescription = new TypeDescription.ForLoadedType(fnClass);

    final String className =
        "auxiliary_OnTimer_" + CharMatcher.JAVA_LETTER_OR_DIGIT.retainFrom(timerId);

    DynamicType.Builder<?> builder =
        new ByteBuddy()
            // Create subclasses inside the target class, to have access to
            // private and package-private bits
            .with(
                new NamingStrategy.SuffixingRandom(className) {
                  @Override
                  public String subclass(TypeDescription.Generic superClass) {
                    return super.name(clazzDescription);
                  }
                })
            // class <invoker class> implements OnTimerInvoker {
            .subclass(OnTimerInvoker.class, ConstructorStrategy.Default.NO_CONSTRUCTORS)

            //   private final <fn class> delegate;
            .defineField(
                FN_DELEGATE_FIELD_NAME, fnClass, Visibility.PRIVATE, FieldManifestation.FINAL)

            //   <invoker class>(<fn class> delegate) { this.delegate = delegate; }
            .defineConstructor(Visibility.PUBLIC)
            .withParameter(fnClass)
            .intercept(new InvokerConstructor())

            //   public invokeOnTimer(ExtraContextFactory) {
            //     this.delegate.<@OnTimer method>(... pass the right args ...)
            //   }
            .method(ElementMatchers.named("invokeOnTimer"))
            .intercept(new InvokeOnTimerDelegation(signature.onTimerMethods().get(timerId)));

    DynamicType.Unloaded<?> unloaded = builder.make();

    @SuppressWarnings("unchecked")
    Class<? extends OnTimerInvoker<?, ?>> res =
        (Class<? extends OnTimerInvoker<?, ?>>)
            unloaded
                .load(
                    OnTimerInvokers.class.getClassLoader(),
                    ClassLoadingStrategy.Default.INJECTION)
                .getLoaded();
    return res;
  }

  /**
   * An "invokeOnTimer" method implementation akin to @ProcessElement, but simpler because no
   * splitting-related parameters need to be handled.
   */
  private static class InvokeOnTimerDelegation extends DoFnMethodDelegation {

    private final DoFnSignature.OnTimerMethod signature;

    public InvokeOnTimerDelegation(DoFnSignature.OnTimerMethod signature) {
      super(signature.targetMethod());
      this.signature = signature;
    }

    @Override
    protected StackManipulation beforeDelegation(MethodDescription instrumentedMethod) {
      // Parameters of the wrapper invoker method:
      //   ExtraContextFactory.
      // Parameters of the wrapped DoFn method:
      //   a dynamic set of allowed "extra" parameters in any order subject to
      //   validation prior to getting the DoFnSignature
      ArrayList<StackManipulation> parameters = new ArrayList<>();
      // Push the extra arguments in their actual order.
      StackManipulation pushExtraContextFactory = MethodVariableAccess.REFERENCE.loadOffset(1);
      for (DoFnSignature.Parameter param : signature.extraParameters()) {
        parameters.add(
            new StackManipulation.Compound(
                pushExtraContextFactory,
                MethodInvocation.invoke(DoFnInvokers.getExtraContextFactoryMethod(param))));
      }
      return new StackManipulation.Compound(parameters);
    }
  }

  /**
   * A constructor {@link Implementation} for a {@link DoFnInvoker class}. Produces the byte code
   * for a constructor that takes a single argument and assigns it to the delegate field.
   */
  private static final class InvokerConstructor implements Implementation {
    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return new ByteCodeAppender() {
        @Override
        public Size apply(
            MethodVisitor methodVisitor,
            Context implementationContext,
            MethodDescription instrumentedMethod) {
          StackManipulation.Size size =
              new StackManipulation.Compound(
                      // Load the this reference
                      MethodVariableAccess.REFERENCE.loadOffset(0),
                      // Invoke the super constructor (default constructor of Object)
                      MethodInvocation.invoke(
                          new TypeDescription.ForLoadedType(Object.class)
                              .getDeclaredMethods()
                              .filter(
                                  ElementMatchers.isConstructor()
                                      .and(ElementMatchers.takesArguments(0)))
                              .getOnly()),
                      // Load the this reference
                      MethodVariableAccess.REFERENCE.loadOffset(0),
                      // Load the delegate argument
                      MethodVariableAccess.REFERENCE.loadOffset(1),
                      // Assign the delegate argument to the delegate field
                      FieldAccess.forField(
                              implementationTarget
                                  .getInstrumentedType()
                                  .getDeclaredFields()
                                  .filter(ElementMatchers.named(FN_DELEGATE_FIELD_NAME))
                                  .getOnly())
                          .putter(),
                      // Return void.
                      MethodReturn.VOID)
                  .apply(methodVisitor, implementationContext);
          return new Size(size.getMaximalSize(), instrumentedMethod.getStackSize());
        }
      };
    }
  }
}
