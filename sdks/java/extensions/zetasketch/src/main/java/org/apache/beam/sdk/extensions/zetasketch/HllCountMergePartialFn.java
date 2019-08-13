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
package org.apache.beam.sdk.extensions.zetasketch;

import com.google.zetasketch.HyperLogLogPlusPlus;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.transforms.Combine;

/**
 * {@link Combine.CombineFn} for the {@link HllCount.MergePartial} combiner.
 *
 * @param <T> type of the HLL++ sketch to be merged
 */
class HllCountMergePartialFn<T> extends Combine.CombineFn<byte[], HyperLogLogPlusPlus<T>, byte[]> {

  // Call HllCountMergePartialFn.create() to instantiate
  private HllCountMergePartialFn() {}

  static HllCountMergePartialFn<?> create() {
    return new HllCountMergePartialFn();
  }

  @Override
  public Coder<HyperLogLogPlusPlus<T>> getAccumulatorCoder(
      CoderRegistry registry, Coder<byte[]> inputCoder) {
    // Use null to represent the "identity element" of the merge operation.
    return NullableCoder.of(HyperLogLogPlusPlusCoder.of());
  }

  @Override
  public HyperLogLogPlusPlus<T> createAccumulator() {
    // Cannot create a sketch corresponding to an empty data set, because we do not know the sketch
    // type and precision. So use null to represent the "identity element" of the merge operation.
    return null;
  }

  @Override
  public HyperLogLogPlusPlus<T> addInput(HyperLogLogPlusPlus<T> accumulator, byte[] input) {
    if (accumulator == null) {
      @SuppressWarnings("unchecked")
      HyperLogLogPlusPlus<T> result = (HyperLogLogPlusPlus<T>) HyperLogLogPlusPlus.forProto(input);
      return result;
    } else {
      accumulator.merge(input);
      return accumulator;
    }
  }

  @Override
  public HyperLogLogPlusPlus<T> mergeAccumulators(Iterable<HyperLogLogPlusPlus<T>> accumulators) {
    HyperLogLogPlusPlus<T> merged = createAccumulator();
    for (HyperLogLogPlusPlus<T> accumulator : accumulators) {
      if (accumulator == null) {
        continue;
      }
      if (merged == null) {
        @SuppressWarnings("unchecked")
        HyperLogLogPlusPlus<T> clonedAccumulator =
            (HyperLogLogPlusPlus<T>) HyperLogLogPlusPlus.forProto(accumulator.serializeToProto());
        // Cannot set merged to accumulator directly because we shouldn't mutate accumulator
        merged = clonedAccumulator;
      } else {
        merged.merge(accumulator);
      }
    }
    return merged;
  }

  @Override
  public byte[] extractOutput(HyperLogLogPlusPlus<T> accumulator) {
    if (accumulator == null) {
      throw new IllegalStateException(
          "HllCountMergePartialFn.extractOutput() should not be called on a null accumulator.");
    }
    return accumulator.serializeToByteArray();
  }
}
