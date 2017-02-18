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
package org.apache.beam.runners.flink.translation.overrides.streaming;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.PCollection;

/**
 * Combiner that combines {@code T}s into a single {@code List<T>} containing all inputs.
 *
 * <p>For internal use by {@link FlinkStreamingViewAsMap}, {@link FlinkStreamingViewAsMultimap},
 * {@link FlinkStreamingViewAsList}, {@link FlinkStreamingViewAsIterable}.
 *
 * <p>They require the input {@link PCollection} fits in memory.
 * For a large {@link PCollection} this is expected to crash!
 *
 * @param <T> the type of elements to concatenate.
 */
class Concatenate<T> extends Combine.CombineFn<T, List<T>, List<T>> {

  private static final long serialVersionUID = 1L;

  @Override
  public List<T> createAccumulator() {
    return new ArrayList<T>();
  }

  @Override
  public List<T> addInput(List<T> accumulator, T input) {
    accumulator.add(input);
    return accumulator;
  }

  @Override
  public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
    List<T> result = createAccumulator();
    for (List<T> accumulator : accumulators) {
      result.addAll(accumulator);
    }
    return result;
  }

  @Override
  public List<T> extractOutput(List<T> accumulator) {
    return accumulator;
  }

  @Override
  public Coder<List<T>> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
    return ListCoder.of(inputCoder);
  }

  @Override
  public Coder<List<T>> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
    return ListCoder.of(inputCoder);
  }
}
