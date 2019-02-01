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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.Contextful.Fn.Context;
import org.apache.beam.sdk.transforms.WithExceptions.ExceptionElement;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/** {@code PTransform}s for mapping a simple function over the elements of a {@link PCollection}. */
public class MapElements<InputT, OutputT>
    extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {
  @Nullable private final transient TypeDescriptor<InputT> inputType;
  @Nullable private final transient TypeDescriptor<OutputT> outputType;
  @Nullable private final transient Object originalFnForDisplayData;
  @Nullable private final Contextful<Fn<InputT, OutputT>> fn;

  private MapElements(
      @Nullable Contextful<Fn<InputT, OutputT>> fn,
      @Nullable Object originalFnForDisplayData,
      @Nullable TypeDescriptor<InputT> inputType,
      TypeDescriptor<OutputT> outputType) {
    this.fn = fn;
    this.originalFnForDisplayData = originalFnForDisplayData;
    this.inputType = inputType;
    this.outputType = outputType;
  }

  /**
   * For {@code InferableFunction<InputT, OutputT>} {@code fn}, returns a {@code PTransform} that
   * takes an input {@code PCollection<InputT>} and returns a {@code PCollection<OutputT>}
   * containing {@code fn.apply(v)} for every element {@code v} in the input.
   *
   * <p>{@link InferableFunction} has the advantage of providing type descriptor information, but it
   * is generally more convenient to specify output type via {@link #into(TypeDescriptor)}, and
   * provide the mapping as a lambda expression to {@link #via(ProcessFunction)}.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * PCollection<String> words = ...;
   * PCollection<Integer> wordsPerLine = words.apply(MapElements.via(
   *     new InferableFunction<String, Integer>() {
   *       public Integer apply(String word) throws Exception {
   *         return word.length();
   *       }
   *     }));
   * }</pre>
   */
  public static <InputT, OutputT> MapElements<InputT, OutputT> via(
      final InferableFunction<InputT, OutputT> fn) {
    return new MapElements<>(
        Contextful.fn(fn), fn, fn.getInputTypeDescriptor(), fn.getOutputTypeDescriptor());
  }

  /**
   * Returns a new {@link MapElements} transform with the given type descriptor for the output type,
   * but the mapping function yet to be specified using {@link #via(ProcessFunction)}.
   */
  public static <OutputT> MapElements<?, OutputT> into(final TypeDescriptor<OutputT> outputType) {
    return new MapElements<>(null, null, null, outputType);
  }

  /**
   * For a {@code ProcessFunction<InputT, OutputT>} {@code fn} and output type descriptor, returns a
   * {@code PTransform} that takes an input {@code PCollection<InputT>} and returns a {@code
   * PCollection<OutputT>} containing {@code fn.apply(v)} for every element {@code v} in the input.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * PCollection<Integer> wordLengths = words.apply(
   *     MapElements.into(TypeDescriptors.integers())
   *                .via((String word) -> word.length()));
   * }</pre>
   */
  public <NewInputT> MapElements<NewInputT, OutputT> via(ProcessFunction<NewInputT, OutputT> fn) {
    return new MapElements<>(Contextful.fn(fn), fn, TypeDescriptors.inputOf(fn), outputType);
  }

  /** Like {@link #via(ProcessFunction)}, but supports access to context, such as side inputs. */
  @Experimental(Kind.CONTEXTFUL)
  public <NewInputT> MapElements<NewInputT, OutputT> via(Contextful<Fn<NewInputT, OutputT>> fn) {
    return new MapElements<>(
        fn, fn.getClosure(), TypeDescriptors.inputOf(fn.getClosure()), outputType);
  }

  @Override
  public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
    checkNotNull(fn, "Must specify a function on MapElements using .via()");
    return input.apply(
        "Map",
        ParDo.of(
                new DoFn<InputT, OutputT>() {
                  @ProcessElement
                  public void processElement(
                      @Element InputT element, OutputReceiver<OutputT> receiver, ProcessContext c)
                      throws Exception {
                    receiver.output(
                        fn.getClosure().apply(element, Fn.Context.wrapProcessContext(c)));
                  }

                  @Override
                  public void populateDisplayData(DisplayData.Builder builder) {
                    builder.delegate(MapElements.this);
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
                        MapElements.class.getSimpleName());
                    return outputType;
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
   * <p>The user must call {@code via} on the returned {@link MapWithExceptions} instance to define
   * an exception handler. If the handler does not provide sufficient type information, the user
   * must also call {@code into} to define a type descriptor for the error collection.
   *
   * <p>See {@link WithExceptions} documentation for usage patterns of the returned {@link
   * WithExceptions.Result}.
   *
   * @return a {@link WithExceptions.Result} wrapping the output and error collections
   */
  public MapWithExceptions<InputT, OutputT, ?> withExceptions() {
    return new MapWithExceptions<>(fn, originalFnForDisplayData, inputType, outputType, null, null);
  }

  /** Implementation of {@link MapElements#withExceptions()}. */
  public static class MapWithExceptions<InputT, OutputT, FailureT>
      extends PTransform<
          PCollection<InputT>, WithExceptions.Result<PCollection<OutputT>, FailureT>> {

    private final transient TypeDescriptor<InputT> inputType;
    private final transient TypeDescriptor<OutputT> outputType;
    @Nullable private final transient TypeDescriptor<FailureT> failureType;
    private final transient Object originalFnForDisplayData;
    private final Contextful<Fn<InputT, OutputT>> fn;
    @Nullable private final ProcessFunction<ExceptionElement<InputT>, FailureT> exceptionHandler;

    MapWithExceptions(
        Contextful<Fn<InputT, OutputT>> fn,
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
     * Returns a new {@link MapWithExceptions} transform with the given type descriptor for the
     * error collection, but the exception handler yet to be specified using {@link
     * #via(ProcessFunction)}.
     */
    public <NewFailureT> MapWithExceptions<InputT, OutputT, NewFailureT> into(
        TypeDescriptor<NewFailureT> failureTypeDescriptor) {
      return new MapWithExceptions<>(
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
     * Result<PCollection<Integer>, String> result = words.apply(
     *     MapElements.into(TypeDescriptors.integers())
     *                .via((String word) -> 1 / word.length())
     *                .withExceptions()
     *                .into(TypeDescriptors.strings())
     *                .via(ee -> e.exception().getMessage()));
     * PCollection<String> errors = result.errors();
     * }</pre>
     */
    public MapWithExceptions<InputT, OutputT, FailureT> via(
        ProcessFunction<ExceptionElement<InputT>, FailureT> exceptionHandler) {
      return new MapWithExceptions<>(
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
     *     MapElements.into(TypeDescriptors.integers())
     *                .via((String word) -> 1 / word.length())
     *                .withExceptions()
     *                .via(new WithExceptions.ExceptionAsMapHandler<String>() {}));
     * PCollection<KV<String, Map<String, String>>> errors = result.errors();
     * }</pre>
     */
    public <NewFailureT> MapWithExceptions<InputT, OutputT, NewFailureT> via(
        InferableFunction<ExceptionElement<InputT>, NewFailureT> exceptionHandler) {
      return new MapWithExceptions<>(
          fn,
          originalFnForDisplayData,
          inputType,
          outputType,
          exceptionHandler,
          exceptionHandler.getOutputTypeDescriptor());
    }

    @Override
    public WithExceptions.Result<PCollection<OutputT>, FailureT> expand(PCollection<InputT> input) {
      final TupleTag<OutputT> outputTag = new TupleTag<OutputT>() {};
      final TupleTag<FailureT> failureTag;
      if (failureType == null) {
        failureTag = new TupleTag<>();
      } else {
        failureTag =
            new TupleTag<FailureT>() {
              @Override
              public TypeDescriptor<FailureT> getTypeDescriptor() {
                return failureType;
              }
            };
      }
      DoFn<InputT, OutputT> doFn =
          new DoFn<InputT, OutputT>() {
            @ProcessElement
            public void processElement(
                @Element InputT element, MultiOutputReceiver receiver, ProcessContext c)
                throws Exception {
              OutputT processed = null;
              boolean exceptionWasThrown = false;
              try {
                processed = fn.getClosure().apply(element, Context.wrapProcessContext(c));
              } catch (Exception e) {
                exceptionWasThrown = true;
                ExceptionElement<InputT> exceptionElement = ExceptionElement.of(element, e);
                receiver.get(failureTag).output(exceptionHandler.apply(exceptionElement));
              }
              if (!exceptionWasThrown) {
                receiver.get(outputTag).output(processed);
              }
            }

            @Override
            public void populateDisplayData(Builder builder) {
              builder.delegate(MapWithExceptions.this);
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
                  MapWithExceptions.class.getSimpleName());
              return outputType;
            }
          };
      PCollectionTuple tuple =
          input.apply(
              MapWithExceptions.class.getSimpleName(),
              ParDo.of(doFn)
                  .withOutputTags(outputTag, TupleTagList.of(failureTag))
                  .withSideInputs(this.fn.getRequirements().getSideInputs()));
      return WithExceptions.Result.of(tuple, outputTag, failureTag);
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
  }
}
