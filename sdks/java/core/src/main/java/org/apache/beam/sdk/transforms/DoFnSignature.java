package org.apache.beam.sdk.transforms;

import static org.apache.beam.sdk.transforms.DoFn.FinishBundle;
import static org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import static org.apache.beam.sdk.transforms.DoFn.StartBundle;
import static org.apache.beam.sdk.transforms.DoFnReflector.format;
import static org.apache.beam.sdk.transforms.DoFnReflector.formatType;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

/**
 * Describes the signature of a {@link DoFn}, in particular, which features it uses, which extra
 * context it requires, types of the input and output elements, etc.
 *
 * <p>See <a href="https://s.apache.org/a-new-dofn">A new DoFn</a>.
 */
class DoFnSignature {
  public final TypeToken<?> inputT;
  public final TypeToken<?> outputT;
  public final ProcessElementMethod processElement;
  public final BundleMethod startBundle;
  public final BundleMethod finishBundle;

  private DoFnSignature(
      TypeToken<?> inputT,
      TypeToken<?> outputT,
      ProcessElementMethod processElement,
      BundleMethod startBundle,
      BundleMethod finishBundle) {
    this.inputT = inputT;
    this.outputT = outputT;
    this.processElement = processElement;
    this.startBundle = startBundle;
    this.finishBundle = finishBundle;
  }

  /** Describes a {@link DoFn.ProcessElement} method. */
  public static class ProcessElementMethod {
    /** The relevant method in the user's class. */
    public final Method targetMethod;
    /** 0-based index of the {@link BoundedWindow} parameter, or -1 if it's not used. */
    public final int boundedWindowParamIndex;
    /** 0-based index of the {@link DoFn.InputProvider} parameter, or -1 if it's not used. */
    public final int inputProviderParamIndex;
    /** 0-based index of the {@link DoFn.OutputReceiver} parameter, or -1 if it's not used. */
    public final int outputReceiverParamIndex;

    private ProcessElementMethod(
        Method targetMethod,
        int boundedWindowParamIndex,
        int inputProviderParamIndex,
        int outputReceiverParamIndex) {
      this.targetMethod = targetMethod;
      this.boundedWindowParamIndex = boundedWindowParamIndex;
      this.inputProviderParamIndex = inputProviderParamIndex;
      this.outputReceiverParamIndex = outputReceiverParamIndex;
    }

    /**
     * Utility method: returns how many parameters the method has, not counting the {@link
     * DoFn.ProcessContext} parameter.
     */
    public int numExtraParameters() {
      int res = 0;
      res += (boundedWindowParamIndex == -1) ? 0 : 1;
      res += (inputProviderParamIndex == -1) ? 0 : 1;
      res += (outputReceiverParamIndex == -1) ? 0 : 1;
      return res;
    }
  }

  /** Describes a {@link DoFn.StartBundle} or {@link DoFn.FinishBundle} method. */
  public static class BundleMethod {
    /** The relevant method in the user's class. */
    public final Method targetMethod;

    private BundleMethod(Method targetMethod) {
      this.targetMethod = targetMethod;
    }
  }

  /** Analyzes a given {@link DoFn} class and extracts its {@link DoFnSignature}. */
  public static DoFnSignature fromFnClass(Class<? extends DoFn> fnClass) {
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

    Method processElementMethod =
        DoFnReflector.findAnnotatedMethod(ProcessElement.class, fnClass, true);
    Method startBundleMethod = DoFnReflector.findAnnotatedMethod(StartBundle.class, fnClass, false);
    Method finishBundleMethod =
        DoFnReflector.findAnnotatedMethod(FinishBundle.class, fnClass, false);

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

  static ProcessElementMethod analyzeProcessElementMethod(
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

    int boundedWindowParamIndex = -1;
    int inputProviderParamIndex = -1;
    int outputReceiverParamIndex = -1;
    TypeToken<?> expectedInputProviderT = inputProviderTypeOf(inputT);
    TypeToken<?> expectedOutputReceiverT = outputReceiverTypeOf(outputT);
    for (int i = 1; i < params.length; ++i) {
      TypeToken<?> param = TypeToken.of(params[i]);
      if (param.isSubtypeOf(BoundedWindow.class)) {
        if (boundedWindowParamIndex != -1) {
          throw new IllegalArgumentException("Multiple BoundedWindow parameters");
        }
        boundedWindowParamIndex = i;
      } else if (param.isSubtypeOf(DoFn.InputProvider.class)) {
        if (inputProviderParamIndex != -1) {
          throw new IllegalArgumentException("Multiple InputProvider parameters");
        }
        if (!param.isSupertypeOf(expectedInputProviderT)) {
          throw new IllegalArgumentException(
              String.format(
                  "Wrong type of InputProvider parameter for method %s: %s, should be %s",
                  format(m), formatType(param), formatType(expectedInputProviderT)));
        }
        inputProviderParamIndex = i;
      } else if (param.isSubtypeOf(DoFn.OutputReceiver.class)) {
        if (outputReceiverParamIndex != -1) {
          throw new IllegalArgumentException("Multiple OutputReceiver parameters");
        }
        if (!param.isSupertypeOf(expectedOutputReceiverT)) {
          throw new IllegalArgumentException(
              String.format(
                  "Wrong type of OutputReceiver parameter for method %s: %s, should be %s",
                  format(m), formatType(param), formatType(expectedOutputReceiverT)));
        }
        outputReceiverParamIndex = i;
      } else {
        List<String> allowedParamTypes =
            Arrays.asList(
                formatType(new TypeToken<BoundedWindow>() {}));
        throw new IllegalArgumentException(
            String.format(
                "%s is not a valid context parameter for method %s. Should be one of %s",
                formatType(param), format(m), allowedParamTypes));
      }
    }

    return new ProcessElementMethod(
        m, boundedWindowParamIndex, inputProviderParamIndex, outputReceiverParamIndex);
  }

  static BundleMethod analyzeBundleMethod(Method m, TypeToken<?> inputT, TypeToken<?> outputT) {
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

    return new BundleMethod(m);
  }
}
