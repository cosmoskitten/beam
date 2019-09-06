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
package org.apache.beam.sdk.extensions.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.InferableFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Requirements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * {@link PTransform} for serializing objects to JSON {@link String Strings}. Transforms a {@code
 * PCollection<InputT>} into a {@link PCollection} of JSON {@link String Strings} representing
 * objects in the original {@link PCollection} using Jackson.
 */
public class AsJsons<InputT> extends PTransform<PCollection<InputT>, PCollection<String>> {
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

  private final Class<? extends InputT> inputClass;
  private ObjectMapper customMapper;

  /**
   * Creates a {@link AsJsons} {@link PTransform} that will transform a {@code PCollection<InputT>}
   * into a {@link PCollection} of JSON {@link String Strings} representing those objects using a
   * Jackson {@link ObjectMapper}.
   */
  public static <OutputT> AsJsons<OutputT> of(Class<? extends OutputT> outputClass) {
    return new AsJsons<>(outputClass);
  }

  private AsJsons(Class<? extends InputT> outputClass) {
    this.inputClass = outputClass;
  }

  /** Use custom Jackson {@link ObjectMapper} instead of the default one. */
  public AsJsons<InputT> withMapper(ObjectMapper mapper) {
    AsJsons<InputT> newTransform = new AsJsons<>(inputClass);
    newTransform.customMapper = mapper;
    return newTransform;
  }

  /**
   * Returns a new {@link AsJsonsWithFailures} transform that catches exceptions raised while
   * writing JSON elements, passing the raised exception instance and the input element being
   * processed through the given {@code exceptionHandler} and emitting the result to a failure
   * collection.
   *
   * <p>See {@link WithFailures} documentation for usage patterns of the returned {@link
   * WithFailures.Result}.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * WithFailures.Result<PCollection<String>, KV<MyPojo, Map<String, String>>> result =
   *     pojos.apply(
   *         AsJsons.of(MyPojo.class)
   *             .withFailures(new WithFailures.ExceptionAsMapHandler<MyPojo>() {}));
   *
   * PCollection<String> output = result.output(); // valid json elements
   * PCollection<KV<MyPojo, Map<String, String>>> failures = result.failures();
   * }</pre>
   */
  @Experimental(Experimental.Kind.WITH_EXCEPTIONS)
  public <FailureT> AsJsonsWithFailures<FailureT> withFailures(
      InferableFunction<WithFailures.ExceptionElement<InputT>, FailureT> exceptionHandler) {
    return new AsJsonsWithFailures<>(exceptionHandler);
  }

  private String writeValue(InputT input) throws JsonProcessingException {
    ObjectMapper mapper = Optional.ofNullable(customMapper).orElse(DEFAULT_MAPPER);
    return mapper.writeValueAsString(input);
  }

  @Override
  public PCollection<String> expand(PCollection<InputT> input) {
    return input.apply(
        MapElements.via(
            new SimpleFunction<InputT, String>() {
              @Override
              public String apply(InputT input) {
                try {
                  return writeValue(input);
                } catch (IOException e) {
                  throw new RuntimeException(
                      "Failed to serialize " + inputClass.getName() + " value: " + input, e);
                }
              }
            }));
  }

  /** A {@code PTransform} that adds exception handling to {@link AsJsons}. */
  public class AsJsonsWithFailures<FailureT>
      extends PTransform<PCollection<InputT>, WithFailures.Result<PCollection<String>, FailureT>> {

    private InferableFunction<WithFailures.ExceptionElement<InputT>, FailureT> exceptionHandler;

    AsJsonsWithFailures(
        InferableFunction<WithFailures.ExceptionElement<InputT>, FailureT> exceptionHandler) {
      this.exceptionHandler = exceptionHandler;
    }

    @Override
    public WithFailures.Result<PCollection<String>, FailureT> expand(PCollection<InputT> input) {
      return input.apply(
          MapElements.into(TypeDescriptors.strings())
              .via(
                  Contextful.fn(
                      (Contextful.Fn<InputT, String>) (input1, c) -> writeValue(input1),
                      Requirements.empty()))
              .exceptionsVia(exceptionHandler));
    }
  }
}
