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

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A collection of utilities for writing transforms that can handle exceptions raised during
 * processing of elements.
 *
 * <p>Consuming transforms such as {@link MapElements.MapWithExceptions} follow the general pattern
 * of taking in a user-defined exception handler of type {@code
 * ProcessFunction<ExceptionElement<InputT>, ErrorOutputT} where the input {@link ExceptionElement}
 * contains an exception along with the input element that was being processed when the exception
 * was raised. This handler is responsible for producing some output element that captures relevant
 * details of the error and can be encoded as part of an error output {@link PCollection}.
 * Transforms can then package together their output and error collections in a {@link
 * WithExceptions.Result} that avoids users needing to interact with {@code TupleTag}s and indexing
 * into a {@link PCollectionTuple}.
 *
 * <p>Exception handlers can narrow their scope by rethrowing the passed {@link
 * ExceptionElement#exception()} and catching only specific subclasses of {@code Exception}.
 * Unhandled exceptions will generally bubble up to a top-level {@link
 * org.apache.beam.sdk.Pipeline.PipelineExecutionException} that halts progress.
 *
 * <p>Users can take advantage of {@link Result#errorsTo(List)} for fluent chaining of transforms
 * that handle exceptions:
 *
 * <pre>{@code
 * PCollection<Integer> input = ...
 * List<PCollection<Map<String, String>> errorCollections = new ArrayList<>();
 * input.apply(MapElements...withExceptions()...)
 *      .errorsTo(errorCollections)
 *      .apply(MapElements...withExceptions()...)
 *      .errorsTo(errorCollections);
 * PCollection<Map<String, String>> errors = PCollectionList.of(errorCollections)
 *      .apply("FlattenErrorCollections", Flatten.pCollections());
 * }</pre>
 */
@Experimental(Experimental.Kind.WITH_EXCEPTIONS)
public class WithExceptions {

  /**
   * The value type passed as input to exception handlers. It wraps an exception together with the
   * input element that was being processed at the time the exception was raised.
   */
  @AutoValue
  public abstract static class ExceptionElement<T> {
    public abstract T element();

    public abstract Exception exception();

    public static <T> ExceptionElement<T> of(T element, Exception exception) {
      return new AutoValue_WithExceptions_ExceptionElement<>(element, exception);
    }
  }

  /**
   * A simple handler that extracts information from an exception to a {@code Map<String, String>}
   * and returns a {@link KV} where the key is the input element that failed processing, and the
   * value is the map of exception attributes.
   *
   * <p>Extends {@link SimpleFunction} so that full type information is captured. Map and {@link KV}
   * coders are well supported by Beam, so coder inference can be successfully applied if the
   * consuming transform passes type information to the error collection's {@link TupleTag}.
   *
   * <p>The keys populated in the map are "className", "message", and "stackTrace" of the exception.
   */
  public static class ExceptionAsMapHandler<T>
      extends SimpleFunction<ExceptionElement<T>, KV<T, Map<String, String>>> {
    @Override
    public KV<T, Map<String, String>> apply(ExceptionElement<T> f) {
      return KV.of(
          f.element(),
          ImmutableMap.of(
              "className", f.exception().getClass().getName(),
              "message", f.exception().getMessage(),
              "stackTrace", Arrays.toString(f.exception().getStackTrace())));
    }
  }

  /**
   * An intermediate output type for PTransforms that allows an output collection to live alongside
   * a collection of elements that failed the transform.
   *
   * @param <OutputT> Output type
   * @param <ErrorElementT> Element type for the error {@code PCollection}
   */
  @AutoValue
  public abstract static class Result<OutputT extends POutput, ErrorElementT>
      implements PInput, POutput {

    public abstract OutputT output();

    @Nullable
    abstract TupleTag<?> outputTag();

    public abstract PCollection<ErrorElementT> errors();

    abstract TupleTag<ErrorElementT> errorsTag();

    public static <OutputT extends POutput, ErrorElementT> Result<OutputT, ErrorElementT> of(
        OutputT output, PCollection<ErrorElementT> errors) {
      return new AutoValue_WithExceptions_Result<>(
          output, null, errors, new TupleTag<ErrorElementT>());
    }

    public static <OutputElementT, ErrorElementT>
        Result<PCollection<OutputElementT>, ErrorElementT> of(
            PCollection<OutputElementT> output, PCollection<ErrorElementT> errors) {
      return new AutoValue_WithExceptions_Result<>(
          output, new TupleTag<OutputElementT>(), errors, new TupleTag<ErrorElementT>());
    }

    public static <OutputElementT, ErrorElementT>
        Result<PCollection<OutputElementT>, ErrorElementT> of(
            PCollectionTuple tuple,
            TupleTag<OutputElementT> outputTag,
            TupleTag<ErrorElementT> errorTag) {
      return new AutoValue_WithExceptions_Result<>(
          tuple.get(outputTag), outputTag, tuple.get(errorTag), errorTag);
    }

    /** Adds the error collection to the passed list and returns just the output collection. */
    public OutputT errorsTo(List<PCollection<ErrorElementT>> errorCollections) {
      errorCollections.add(errors());
      return output();
    }

    @Override
    public Pipeline getPipeline() {
      return output().getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      Map<TupleTag<?>, PValue> values = new HashMap<>();
      values.put(errorsTag(), errors());
      if (outputTag() != null && output() instanceof PValue) {
        values.put(outputTag(), (PValue) output());
      }
      return values;
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {}
  }
}
