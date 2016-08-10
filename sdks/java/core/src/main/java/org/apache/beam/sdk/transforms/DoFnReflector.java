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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn.ExtraContextFactory;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.common.reflect.TypeToken;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.NamingStrategy.SuffixingRandom;
import net.bytebuddy.description.field.FieldDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.Generic;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy.Default;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodCall.MethodLocator;
import net.bytebuddy.implementation.bind.MethodDelegationBinder.MethodInvoker;
import net.bytebuddy.implementation.bind.annotation.TargetMethodAnnotationDrivenBinder.TerminationHandler;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.Throw;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.matcher.ElementMatchers;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import javax.annotation.Nullable;

/** Utility implementing the necessary reflection for working with {@link DoFn}s. */
public abstract class DoFnReflector {

  private static final String FN_DELEGATE_FIELD_NAME = "delegate";

  static Collection<Method> declaredMethodsWithAnnotation(
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

  static Method findAnnotatedMethod(
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

  /** @return true if the reflected {@link DoFn} uses a Single Window. */
  public abstract boolean usesSingleWindow();

  /** Create an {@link DoFnInvoker} bound to the given {@link OldDoFn}. */
  public abstract <InputT, OutputT> DoFnInvoker<InputT, OutputT> bindInvoker(
      DoFn<InputT, OutputT> fn);

  private static final Map<Class<?>, DoFnReflector> REFLECTOR_CACHE = new LinkedHashMap<>();

  /** @return the {@link DoFnReflector} for the given {@link DoFn}. */
  public static DoFnReflector of(@SuppressWarnings("rawtypes") Class<? extends DoFn> fn) {
    DoFnReflector reflector = REFLECTOR_CACHE.get(fn);
    if (reflector != null) {
      return reflector;
    }

    reflector = new GenericDoFnReflector(fn);
    REFLECTOR_CACHE.put(fn, reflector);
    return reflector;
  }

  /** Create a {@link OldDoFn} that the {@link DoFn}. */
  public <InputT, OutputT> OldDoFn<InputT, OutputT> toDoFn(DoFn<InputT, OutputT> fn) {
    if (usesSingleWindow()) {
      return new WindowDoFnAdapter<>(this, fn);
    } else {
      return new SimpleDoFnAdapter<>(this, fn);
    }
  }

  static String format(Method m) {
    return ReflectHelpers.CLASS_AND_METHOD_FORMATTER.apply(m);
  }

  static String formatType(TypeToken<?> t) {
    return ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(t.getType());
  }

  /** Interface for invoking the {@code OldDoFn} processing methods. */
  public interface DoFnInvoker<InputT, OutputT> {
    /** Invoke {@link OldDoFn#startBundle} on the bound {@code OldDoFn}. */
    void invokeStartBundle(DoFn<InputT, OutputT>.Context c);
    /** Invoke {@link OldDoFn#finishBundle} on the bound {@code OldDoFn}. */
    void invokeFinishBundle(DoFn<InputT, OutputT>.Context c);

    /** Invoke {@link OldDoFn#processElement} on the bound {@code OldDoFn}. */
    void invokeProcessElement(
        DoFn<InputT, OutputT>.ProcessContext c, ExtraContextFactory<InputT, OutputT> extra);
  }

  /** Implementation of {@link DoFnReflector} for the arbitrary {@link DoFn}. */
  private static class GenericDoFnReflector extends DoFnReflector {
    private final DoFnSignature signature;
    private final Constructor<?> constructor;

    private GenericDoFnReflector(@SuppressWarnings("rawtypes") Class<? extends DoFn> fnClass) {
      this.signature = DoFnSignature.fromFnClass(fnClass);

      Class<? extends DoFnInvoker<?, ?>> wrapperClass = createWrapperClass(fnClass);
      try {
        this.constructor = wrapperClass.getConstructor(fnClass);
      } catch (IllegalArgumentException | NoSuchMethodException | SecurityException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean usesSingleWindow() {
      return signature.processElement.boundedWindowParamIndex != -1;
    }

    private Class<? extends DoFnInvoker<?, ?>> createWrapperClass(Class<? extends DoFn> clazz) {
      final TypeDescription clazzDescription = new TypeDescription.ForLoadedType(clazz);

      DynamicType.Builder<?> builder =
          new ByteBuddy()
              // Create subclasses inside the target class, to have access to
              // private and package-private bits
              .with(
                  new SuffixingRandom("auxiliary") {
                    @Override
                    public String subclass(Generic superClass) {
                      return super.name(clazzDescription);
                    }
                  })
              // Create a subclass of DoFnInvoker
              .subclass(DoFnInvoker.class, Default.NO_CONSTRUCTORS)
              .defineField(
                  FN_DELEGATE_FIELD_NAME, clazz, Visibility.PRIVATE, FieldManifestation.FINAL)
              .defineConstructor(Visibility.PUBLIC)
              .withParameter(clazz)
              .intercept(new InvokerConstructor())
              // Delegate processElement(), startBundle() and finishBundle() to the fn.
              .method(ElementMatchers.named("invokeProcessElement"))
              .intercept(new ProcessElementDelegation(signature.processElement))
              .method(ElementMatchers.named("invokeStartBundle"))
              .intercept(new BundleMethodDelegation(signature.startBundle))
              .method(ElementMatchers.named("invokeFinishBundle"))
              .intercept(new BundleMethodDelegation(signature.finishBundle));

      DynamicType.Unloaded<?> unloaded = builder.make();

      @SuppressWarnings("unchecked")
      Class<? extends DoFnInvoker<?, ?>> res =
          (Class<? extends DoFnInvoker<?, ?>>)
              unloaded
                  .load(getClass().getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                  .getLoaded();
      return res;
    }

    @Override
    public <InputT, OutputT> DoFnInvoker<InputT, OutputT> bindInvoker(DoFn<InputT, OutputT> fn) {
      try {
        @SuppressWarnings("unchecked")
        DoFnInvoker<InputT, OutputT> invoker =
            (DoFnInvoker<InputT, OutputT>) constructor.newInstance(fn);
        return invoker;
      } catch (InstantiationException
          | IllegalAccessException
          | IllegalArgumentException
          | InvocationTargetException
          | SecurityException e) {
        throw new RuntimeException("Unable to bind invoker for " + fn.getClass(), e);
      }
    }
  }

  private static class ContextAdapter<InputT, OutputT> extends DoFn<InputT, OutputT>.Context
      implements DoFn.ExtraContextFactory<InputT, OutputT> {

    private OldDoFn<InputT, OutputT>.Context context;

    private ContextAdapter(DoFn<InputT, OutputT> fn, OldDoFn<InputT, OutputT>.Context context) {
      fn.super();
      this.context = context;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return context.getPipelineOptions();
    }

    @Override
    public void output(OutputT output) {
      context.output(output);
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      context.outputWithTimestamp(output, timestamp);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      context.sideOutput(tag, output);
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      context.sideOutputWithTimestamp(tag, output, timestamp);
    }

    @Override
    public BoundedWindow window() {
      // The DoFn doesn't allow us to ask for these outside ProcessElements, so this
      // should be unreachable.
      throw new UnsupportedOperationException("Can only get the window in ProcessElements");
    }

    @Override
    public DoFn.InputProvider<InputT> inputProvider() {
      throw new UnsupportedOperationException("inputProvider() exists only for testing");
    }

    @Override
    public DoFn.OutputReceiver<OutputT> outputReceiver() {
      throw new UnsupportedOperationException("outputReceiver() exists only for testing");
    }
  }

  private static class ProcessContextAdapter<InputT, OutputT>
      extends DoFn<InputT, OutputT>.ProcessContext
      implements DoFn.ExtraContextFactory<InputT, OutputT> {

    private OldDoFn<InputT, OutputT>.ProcessContext context;

    private ProcessContextAdapter(
        DoFn<InputT, OutputT> fn, OldDoFn<InputT, OutputT>.ProcessContext context) {
      fn.super();
      this.context = context;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return context.getPipelineOptions();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      return context.sideInput(view);
    }

    @Override
    public void output(OutputT output) {
      context.output(output);
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      context.outputWithTimestamp(output, timestamp);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      context.sideOutput(tag, output);
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      context.sideOutputWithTimestamp(tag, output, timestamp);
    }

    @Override
    public InputT element() {
      return context.element();
    }

    @Override
    public Instant timestamp() {
      return context.timestamp();
    }

    @Override
    public PaneInfo pane() {
      return context.pane();
    }

    @Override
    public BoundedWindow window() {
      return context.window();
    }

    @Override
    public DoFn.InputProvider<InputT> inputProvider() {
      throw new UnsupportedOperationException("inputProvider() exists only for testing");
    }

    @Override
    public DoFn.OutputReceiver<OutputT> outputReceiver() {
      throw new UnsupportedOperationException("outputReceiver() exists only for testing");
    }
  }

  static Class<?> getDoFnClass(OldDoFn<?, ?> fn) {
    if (fn instanceof SimpleDoFnAdapter) {
      return ((SimpleDoFnAdapter<?, ?>) fn).fn.getClass();
    } else {
      return fn.getClass();
    }
  }

  private static class SimpleDoFnAdapter<InputT, OutputT> extends OldDoFn<InputT, OutputT> {

    private final DoFn<InputT, OutputT> fn;
    private transient DoFnInvoker<InputT, OutputT> invoker;

    private SimpleDoFnAdapter(DoFnReflector reflector, DoFn<InputT, OutputT> fn) {
      super(fn.aggregators);
      this.fn = fn;
      this.invoker = reflector.bindInvoker(fn);
    }

    @Override
    public void startBundle(OldDoFn<InputT, OutputT>.Context c) throws Exception {
      this.fn.prepareForProcessing();
      invoker.invokeStartBundle(new ContextAdapter<>(fn, c));
    }

    @Override
    public void finishBundle(OldDoFn<InputT, OutputT>.Context c) throws Exception {
      invoker.invokeFinishBundle(new ContextAdapter<>(fn, c));
    }

    @Override
    public void processElement(OldDoFn<InputT, OutputT>.ProcessContext c) throws Exception {
      ProcessContextAdapter<InputT, OutputT> adapter = new ProcessContextAdapter<>(fn, c);
      invoker.invokeProcessElement(adapter, adapter);
    }

    @Override
    protected TypeDescriptor<InputT> getInputTypeDescriptor() {
      return fn.getInputTypeDescriptor();
    }

    @Override
    protected TypeDescriptor<OutputT> getOutputTypeDescriptor() {
      return fn.getOutputTypeDescriptor();
    }

    @Override
    public Duration getAllowedTimestampSkew() {
      return fn.getAllowedTimestampSkew();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.include(fn);
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      invoker = DoFnReflector.of(fn.getClass()).bindInvoker(fn);
    }
  }

  private static class WindowDoFnAdapter<InputT, OutputT> extends SimpleDoFnAdapter<InputT, OutputT>
      implements OldDoFn.RequiresWindowAccess {

    private WindowDoFnAdapter(DoFnReflector reflector, DoFn<InputT, OutputT> fn) {
      super(reflector, fn);
    }
  }

  private abstract static class MethodDelegation implements Implementation {
    FieldDescription delegateField;

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      // Remember the field description of the instrumented type.
      delegateField =
          instrumentedType
              .getDeclaredFields()
              .filter(ElementMatchers.named(FN_DELEGATE_FIELD_NAME))
              .getOnly();

      // Delegating the method call doesn't require any changes to the instrumented type.
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
          StackManipulation manipulation =
              isRequired()
                  ? new StackManipulation.Compound(
                      // Push "this" reference to the stack
                      MethodVariableAccess.REFERENCE.loadOffset(0),
                      // Access the delegate field of the the invoker
                      FieldAccess.forField(delegateField).getter(),
                      invokeTargetMethod(instrumentedMethod))
                  : MethodReturn.VOID;
          StackManipulation.Size size = manipulation.apply(methodVisitor, implementationContext);
          return new Size(size.getMaximalSize(), instrumentedMethod.getStackSize());
        }
      };
    }

    protected abstract boolean isRequired();

    protected abstract StackManipulation invokeTargetMethod(MethodDescription instrumentedMethod);
  }

  private static final class ProcessElementDelegation extends MethodDelegation {
    private static final MethodDescription WINDOW_METHOD;
    private static final MethodDescription INPUT_PROVIDER_METHOD;
    private static final MethodDescription OUTPUT_RECEIVER_METHOD;

    static {
      try {
        WINDOW_METHOD =
            new MethodDescription.ForLoadedMethod(ExtraContextFactory.class.getMethod("window"));
        INPUT_PROVIDER_METHOD =
            new MethodDescription.ForLoadedMethod(
                ExtraContextFactory.class.getMethod("inputProvider"));
        OUTPUT_RECEIVER_METHOD =
            new MethodDescription.ForLoadedMethod(
                ExtraContextFactory.class.getMethod("outputReceiver"));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private final DoFnSignature.ProcessElementMethod signature;

    private ProcessElementDelegation(DoFnSignature.ProcessElementMethod signature) {
      this.signature = signature;
    }

    @Override
    protected boolean isRequired() {
      return true;
    }

    @Override
    protected StackManipulation invokeTargetMethod(MethodDescription instrumentedMethod) {
      MethodDescription targetMethod =
          new MethodLocator.ForExplicitMethod(
                  new MethodDescription.ForLoadedMethod(signature.targetMethod))
              .resolve(instrumentedMethod);

      // Parameters of the wrapper OldDoFn method:
      //   DoFn.ProcessContext, ExtraContextFactory.
      // Parameters of the wrapped DoFn method:
      //   DoFn.ProcessContext, [BoundedWindow, InputProvider, OutputReceiver] in any order
      ArrayList<StackManipulation> parameters = new ArrayList<>();
      // Push the ProcessContext argument.
      parameters.add(MethodVariableAccess.REFERENCE.loadOffset(1));
      // Push the extra arguments in their actual order.
      StackManipulation pushExtraContextFactory = MethodVariableAccess.REFERENCE.loadOffset(2);
      for (int i = 1; i < signature.numExtraParameters() + 1; ++i) {
        if (i == signature.boundedWindowParamIndex) {
          parameters.add(
              new StackManipulation.Compound(
                  pushExtraContextFactory, MethodInvocation.invoke(WINDOW_METHOD)));
        } else if (i == signature.inputProviderParamIndex) {
          parameters.add(
              new StackManipulation.Compound(
                  pushExtraContextFactory, MethodInvocation.invoke(INPUT_PROVIDER_METHOD)));
        } else if (i == signature.outputReceiverParamIndex) {
          parameters.add(
              new StackManipulation.Compound(
                  pushExtraContextFactory, MethodInvocation.invoke(OUTPUT_RECEIVER_METHOD)));
        } else {
          throw new IllegalStateException("Unexpected parameter #" + i);
        }
      }

      return new StackManipulation.Compound(
          // Push the parameters
          new StackManipulation.Compound(parameters),
          // Invoke the target method
          wrapWithUserCodeException(MethodInvoker.Simple.INSTANCE.invoke(targetMethod)),
          // Return from the instrumented method
          TerminationHandler.Returning.INSTANCE.resolve(
              Assigner.DEFAULT, instrumentedMethod, targetMethod));
    }
  }

  private static final class BundleMethodDelegation extends MethodDelegation {
    private final DoFnSignature.BundleMethod signature;

    private BundleMethodDelegation(@Nullable DoFnSignature.BundleMethod signature) {
      this.signature = signature;
    }

    @Override
    protected boolean isRequired() {
      return signature != null;
    }

    @Override
    protected StackManipulation invokeTargetMethod(MethodDescription instrumentedMethod) {
      MethodDescription targetMethod =
          new MethodLocator.ForExplicitMethod(
                  new MethodDescription.ForLoadedMethod(checkNotNull(signature).targetMethod))
              .resolve(instrumentedMethod);
      return new StackManipulation.Compound(
          // Push the parameters
          MethodVariableAccess.REFERENCE.loadOffset(1),
          // Invoke the target method
          wrapWithUserCodeException(MethodInvoker.Simple.INSTANCE.invoke(targetMethod)),
          // Return from the instrumented method
          TerminationHandler.Returning.INSTANCE.resolve(
              Assigner.DEFAULT, instrumentedMethod, targetMethod));
    }
  }

  /**
   * Wrap a given stack manipulation in a try catch block. Any exceptions thrown within the try are
   * wrapped with a {@link UserCodeException}.
   */
  private static StackManipulation wrapWithUserCodeException(final StackManipulation tryBody) {
    final MethodDescription createUserCodeException;
    try {
      createUserCodeException =
          new MethodDescription.ForLoadedMethod(
              UserCodeException.class.getDeclaredMethod("wrap", Throwable.class));
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException("Unable to find UserCodeException.wrap", e);
    }

    return new StackManipulation() {
      @Override
      public boolean isValid() {
        return tryBody.isValid();
      }

      @Override
      public Size apply(MethodVisitor mv, Implementation.Context implementationContext) {
        Label tryBlockStart = new Label();
        Label tryBlockEnd = new Label();
        Label catchBlockStart = new Label();
        Label catchBlockEnd = new Label();

        String throwableName = new TypeDescription.ForLoadedType(Throwable.class).getInternalName();
        mv.visitTryCatchBlock(tryBlockStart, tryBlockEnd, catchBlockStart, throwableName);

        // The try block attempts to perform the expected operations, then jumps to success
        mv.visitLabel(tryBlockStart);
        Size trySize = tryBody.apply(mv, implementationContext);
        mv.visitJumpInsn(Opcodes.GOTO, catchBlockEnd);
        mv.visitLabel(tryBlockEnd);

        // The handler wraps the exception, and then throws.
        mv.visitLabel(catchBlockStart);
        // Add the exception to the frame
        mv.visitFrame(
            Opcodes.F_SAME1,
            // No local variables
            0,
            new Object[] {},
            // 1 stack element (the throwable)
            1,
            new Object[] {throwableName});

        Size catchSize =
            new Compound(MethodInvocation.invoke(createUserCodeException), Throw.INSTANCE)
                .apply(mv, implementationContext);

        mv.visitLabel(catchBlockEnd);
        // The frame contents after the try/catch block is the same
        // as it was before.
        mv.visitFrame(
            Opcodes.F_SAME,
            // No local variables
            0,
            new Object[] {},
            // No new stack variables
            0,
            new Object[] {});

        return new Size(
            trySize.getSizeImpact(),
            Math.max(trySize.getMaximalSize(), catchSize.getMaximalSize()));
      }
    };
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
