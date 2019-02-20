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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.WithExceptions.ExceptionElement;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * {@code PTransform}s for mapping a simple function that returns iterables over the elements of a
 * {@link PCollection} and merging the results.
 */
public class FlatMapElements<InputT, OutputT>
    extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {
  @Nullable private final transient TypeDescriptor<InputT> inputType;
  @Nullable private final transient TypeDescriptor<OutputT> outputType;
  @Nullable private final transient Object originalFnForDisplayData;
  @Nullable private final Contextful<Fn<InputT, Iterable<OutputT>>> fn;

  private FlatMapElements(
      @Nullable Contextful<Fn<InputT, Iterable<OutputT>>> fn,
      @Nullable Object originalFnForDisplayData,
      @Nullable TypeDescriptor<InputT> inputType,
      TypeDescriptor<OutputT> outputType) {
    this.fn = fn;
    this.originalFnForDisplayData = originalFnForDisplayData;
    this.inputType = inputType;
    this.outputType = outputType;
  }

  /**
   * For a {@code InferableFunction<InputT, ? extends Iterable<OutputT>>} {@code fn}, return a
   * {@link PTransform} that applies {@code fn} to every element of the input {@code
   * PCollection<InputT>} and outputs all of the elements to the output {@code
   * PCollection<OutputT>}.
   *
   * <p>{@link InferableFunction} has the advantage of providing type descriptor information, but it
   * is generally more convenient to specify output type via {@link #into(TypeDescriptor)}, and
   * provide the mapping as a lambda expression to {@link #via(ProcessFunction)}.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * PCollection<String> lines = ...;
   * PCollection<String> words = lines.apply(FlatMapElements.via(
   *     new InferableFunction<String, List<String>>() {
   *       public Integer apply(String line) throws Exception {
   *         return Arrays.asList(line.split(" "));
   *       }
   *     });
   * }</pre>
   */
  public static <InputT, OutputT> FlatMapElements<InputT, OutputT> via(
      InferableFunction<? super InputT, ? extends Iterable<OutputT>> fn) {
    Contextful<Fn<InputT, Iterable<OutputT>>> wrapped = (Contextful) Contextful.fn(fn);
    TypeDescriptor<OutputT> outputType =
        TypeDescriptors.extractFromTypeParameters(
            (TypeDescriptor<Iterable<OutputT>>) fn.getOutputTypeDescriptor(),
            Iterable.class,
            new TypeDescriptors.TypeVariableExtractor<Iterable<OutputT>, OutputT>() {});
    TypeDescriptor<InputT> inputType = (TypeDescriptor<InputT>) fn.getInputTypeDescriptor();
    return new FlatMapElements<>(wrapped, fn, inputType, outputType);
  }

  /**
   * Returns a new {@link FlatMapElements} transform with the given type descriptor for the output
   * type, but the mapping function yet to be specified using {@link #via(ProcessFunction)}.
   */
  public static <OutputT> FlatMapElements<?, OutputT> into(
      final TypeDescriptor<OutputT> outputType) {
    return new FlatMapElements<>(null, null, null, outputType);
  }

  /**
   * For a {@code ProcessFunction<InputT, ? extends Iterable<OutputT>>} {@code fn}, returns a {@link
   * PTransform} that applies {@code fn} to every element of the input {@code PCollection<InputT>}
   * and outputs all of the elements to the output {@code PCollection<OutputT>}.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * PCollection<String> words = lines.apply(
   *     FlatMapElements.into(TypeDescriptors.strings())
   *                    .via((String line) -> Arrays.asList(line.split(" ")))
   * }</pre>
   */
  public <NewInputT> FlatMapElements<NewInputT, OutputT> via(
      ProcessFunction<NewInputT, ? extends Iterable<OutputT>> fn) {
    return new FlatMapElements<>(
        (Contextful) Contextful.fn(fn), fn, TypeDescriptors.inputOf(fn), outputType);
  }

  /** Like {@link #via(ProcessFunction)}, but allows access to additional context. */
  @Experimental(Experimental.Kind.CONTEXTFUL)
  public <NewInputT> FlatMapElements<NewInputT, OutputT> via(
      Contextful<Fn<NewInputT, Iterable<OutputT>>> fn) {
    return new FlatMapElements<>(
        fn, fn.getClosure(), TypeDescriptors.inputOf(fn.getClosure()), outputType);
  }

  @Override
  public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
    checkArgument(fn != null, ".via() is required");
    return input.apply(
        "FlatMap",
        ParDo.of(
                new DoFn<InputT, OutputT>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) throws Exception {
                    Iterable<OutputT> res =
                        fn.getClosure().apply(c.element(), Fn.Context.wrapProcessContext(c));
                    for (OutputT output : res) {
                      c.output(output);
                    }
                  }

                  @Override
                  public TypeDescriptor<InputT> getInputTypeDescriptor() {
                    return inputType;
                  }

                  @Override
                  public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
                    checkState(
                        outputType != null,
                        "%s output type descriptor was null; "
                            + "this probably means that getOutputTypeDescriptor() was called after "
                            + "serialization/deserialization, but it is only available prior to "
                            + "serialization, for constructing a pipeline and inferring coders",
                        FlatMapElements.class.getSimpleName());
                    return outputType;
                  }

                  @Override
                  public void populateDisplayData(DisplayData.Builder builder) {
                    builder.delegate(FlatMapElements.this);
                  }
                })
            .withSideInputs(fn.getRequirements().getSideInputs()));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("class", originalFnForDisplayData.getClass()));
    if (originalFnForDisplayData instanceof HasDisplayData) {
      builder.include("fn", (HasDisplayData) originalFnForDisplayData);
    }
  }

  /**
   * Return a modified {@code PTransform} that catches exceptions raised while mapping elements.
   *
   * <p>The user must call {@code via} on the returned {@link FlatMapWithExceptions} instance to
   * define an exception handler. If the handler does not provide sufficient type information, the
   * user must also call {@code into} to define a type descriptor for the error collection.
   *
   * <p>See {@link WithExceptions} documentation for usage patterns of the returned {@link
   * WithExceptions.Result}.
   *
   * @return a {@link WithExceptions.Result} wrapping the output and error collections
   */
  @Experimental(Experimental.Kind.WITH_EXCEPTIONS)
  public FlatMapWithExceptions<InputT, OutputT, ?> withExceptions() {
    return new FlatMapWithExceptions<>(
        fn, originalFnForDisplayData, inputType, outputType, null, null);
  }

  /** Implementation of {@link FlatMapElements#withExceptions()}. */
  @Experimental(Experimental.Kind.WITH_EXCEPTIONS)
  public static class FlatMapWithExceptions<InputT, OutputT, FailureT>
      extends PTransform<
          PCollection<InputT>, WithExceptions.Result<PCollection<OutputT>, FailureT>> {

    private final transient TypeDescriptor<InputT> inputType;
    private final transient TypeDescriptor<OutputT> outputType;
    @Nullable private final transient TypeDescriptor<FailureT> failureType;
    private final transient Object originalFnForDisplayData;
    @Nullable private final Contextful<Fn<InputT, Iterable<OutputT>>> fn;
    @Nullable private final ProcessFunction<ExceptionElement<InputT>, FailureT> exceptionHandler;

    FlatMapWithExceptions(
        @Nullable Contextful<Fn<InputT, Iterable<OutputT>>> fn,
        Object originalFnForDisplayData,
        TypeDescriptor<InputT> inputType,
        TypeDescriptor<OutputT> outputType,
        @Nullable ProcessFunction<ExceptionElement<InputT>, FailureT> exceptionHandler,
        @Nullable TypeDescriptor<FailureT> failureType) {
      this.fn = fn;
      this.originalFnForDisplayData = originalFnForDisplayData;
      this.inputType = inputType;
      this.outputType = outputType;
      this.exceptionHandler = exceptionHandler;
      this.failureType = failureType;
    }

    /**
     * Returns a new {@link FlatMapWithExceptions} transform with the given type descriptor for the
     * error collection, but the exception handler yet to be specified using {@link
     * #via(ProcessFunction)}.
     */
    public <NewFailureT> FlatMapWithExceptions<InputT, OutputT, NewFailureT> into(
        TypeDescriptor<NewFailureT> failureTypeDescriptor) {
      return new FlatMapWithExceptions<>(
          fn, originalFnForDisplayData, inputType, outputType, null, failureTypeDescriptor);
    }

    /**
     * Returns a {@code PTransform} that catches exceptions raised while mapping elements, passing
     * the raised exception instance and the input element being processed through the given {@code
     * exceptionHandler} and emitting the result to an error collection.
     *
     * <p>Example usage:
     *
     * <pre>{@code
     * Result<PCollection<String>, String>> result = words.apply(
     *     FlatMapElements.into(TypeDescriptors.strings())
     *         .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5)))
     *         .withExceptions()
     *         .into(TypeDescriptors.strings())
     *         .via(ee -> e.exception().getMessage()));
     * PCollection<String> errors = result.errors();
     * }</pre>
     */
    public FlatMapWithExceptions<InputT, OutputT, FailureT> via(
        ProcessFunction<ExceptionElement<InputT>, FailureT> exceptionHandler) {
      return new FlatMapWithExceptions<>(
          fn, originalFnForDisplayData, inputType, outputType, exceptionHandler, failureType);
    }

    /**
     * Like {@link #via(ProcessFunction)}, but takes advantage of the type information provided by
     * {@link InferableFunction}, meaning that a call to {@link #into(TypeDescriptor)} may not be
     * necessary.
     *
     * <p>Example usage:
     *
     * <pre>{@code
     * Result<PCollection<Integer>, KV<String, Map<String, String>>> result = words.apply(
     *     FlatMapElements.into(TypeDescriptors.strings())
     *         .via((String line) -> Arrays.asList(Arrays.copyOfRange(line.split(" "), 1, 5)))
     *         .withExceptions()
     *         .via(new WithExceptions.ExceptionAsMapHandler<String>() {}));
     * PCollection<KV<String, Map<String, String>>> errors = result.errors();
     * }</pre>
     */
    public <NewFailureT> FlatMapWithExceptions<InputT, OutputT, NewFailureT> via(
        InferableFunction<ExceptionElement<InputT>, NewFailureT> exceptionHandler) {
      return new FlatMapWithExceptions<>(
          fn,
          originalFnForDisplayData,
          inputType,
          outputType,
          exceptionHandler,
          exceptionHandler.getOutputTypeDescriptor());
    }

    @Override
    public WithExceptions.Result<PCollection<OutputT>, FailureT> expand(PCollection<InputT> input) {
      MapFn doFn = new MapFn();
      PCollectionTuple tuple =
          input.apply(
              FlatMapWithExceptions.class.getSimpleName(),
              ParDo.of(doFn)
                  .withOutputTags(doFn.outputTag, TupleTagList.of(doFn.errorTag))
                  .withSideInputs(this.fn.getRequirements().getSideInputs()));
      return WithExceptions.Result.of(tuple, doFn.outputTag, doFn.errorTag);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("class", originalFnForDisplayData.getClass()));
      if (originalFnForDisplayData instanceof HasDisplayData) {
        builder.include("fn", (HasDisplayData) originalFnForDisplayData);
      }
      builder.add(DisplayData.item("exceptionHandler.class", exceptionHandler.getClass()));
      if (exceptionHandler instanceof HasDisplayData) {
        builder.include("exceptionHandler", (HasDisplayData) exceptionHandler);
      }
    }

    class ErrorTag extends TupleTag<FailureT> {
      @Override
      public TypeDescriptor<FailureT> getTypeDescriptor() {
        return failureType;
      }
    }

    class MapFn extends DoFn<InputT, OutputT> {

      final TupleTag<OutputT> outputTag = new TupleTag<OutputT>() {};
      final TupleTag<FailureT> errorTag = (failureType == null) ? new TupleTag<>() : new ErrorTag();

      @ProcessElement
      public void processElement(
          @Element InputT element, MultiOutputReceiver r, ProcessContext c) throws Exception {
        boolean exceptionWasThrown = false;
        Iterable<OutputT> res = null;
        try {
          res = fn.getClosure().apply(c.element(), Fn.Context.wrapProcessContext(c));
        } catch (Exception e) {
          exceptionWasThrown = true;
          ExceptionElement<InputT> exceptionElement = ExceptionElement.of(element, e);
          r.get(errorTag).output(exceptionHandler.apply(exceptionElement));
        }
        if (!exceptionWasThrown) {
          for (OutputT output : res) {
            r.get(outputTag).output(output);
          }
        }
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.delegate(FlatMapWithExceptions.this);
      }

      @Override
      public TypeDescriptor<InputT> getInputTypeDescriptor() {
        return inputType;
      }

      @Override
      public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
        checkState(
            outputType != null,
            "%s output type descriptor was null; "
                + "this probably means that getOutputTypeDescriptor() was called after "
                + "serialization/deserialization, but it is only available prior to "
                + "serialization, for constructing a pipeline and inferring coders",
            FlatMapWithExceptions.class.getSimpleName());
        return outputType;
      }
    }

  }
}
