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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.zetasketch.HllCountMergePartialFn.HyperLogLogPlusPlusWrapper;
import org.apache.beam.sdk.transforms.Combine;

/**
 * {@link Combine.CombineFn} for the {@link HllCount.MergePartial} combiner.
 *
 * @param <T> type of the HLL++ sketch to be merged
 */
class HllCountMergePartialFn<T>
    extends Combine.CombineFn<byte[], HyperLogLogPlusPlusWrapper<T>, byte[]> {

  // Call HllCountMergePartialFn.create() to instantiate
  private HllCountMergePartialFn() {}

  static HllCountMergePartialFn<?> create() {
    return new HllCountMergePartialFn();
  }

  /**
   * Accumulator for the {@link HllCount.MergePartial} combiner. Cannot use {@link
   * HyperLogLogPlusPlus} directly because we need an "identity element" for the {@code merge}
   * operation, and we are not able to create such "identity element" of type {@link
   * HyperLogLogPlusPlus} without knowing the sketch precision and value type.
   */
  static final class HyperLogLogPlusPlusWrapper<T> {
    @Nullable private HyperLogLogPlusPlus<T> hll;

    private HyperLogLogPlusPlusWrapper(@Nullable HyperLogLogPlusPlus<T> hll) {
      this.hll = hll;
    }
  }

  /**
   * Coder for the {@link HyperLogLogPlusPlusWrapper} class with generic type parameter {@code T}.
   */
  private static final class HyperLogLogPlusPlusWrapperCoder<T>
      extends AtomicCoder<HyperLogLogPlusPlusWrapper<T>> {

    private static final HyperLogLogPlusPlusWrapperCoder<?> INSTANCE =
        new HyperLogLogPlusPlusWrapperCoder<>();

    private final Coder<HyperLogLogPlusPlus<T>> nullableHllCoder =
        NullableCoder.of(HyperLogLogPlusPlusCoder.of());

    // Generic singleton factory pattern; the coder works for all HyperLogLogPlusPlusWrapper objects
    // at runtime regardless of type T
    @SuppressWarnings("unchecked")
    static <T> HyperLogLogPlusPlusWrapperCoder<T> of() {
      return (HyperLogLogPlusPlusWrapperCoder<T>) INSTANCE;
    }

    @Override
    public void encode(HyperLogLogPlusPlusWrapper<T> value, OutputStream outStream)
        throws IOException {
      nullableHllCoder.encode(value.hll, outStream);
    }

    @Override
    public HyperLogLogPlusPlusWrapper<T> decode(InputStream inStream) throws IOException {
      return new HyperLogLogPlusPlusWrapper<T>(nullableHllCoder.decode(inStream));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      nullableHllCoder.verifyDeterministic();
    }
  }

  @Override
  public Coder<HyperLogLogPlusPlusWrapper<T>> getAccumulatorCoder(
      CoderRegistry registry, Coder<byte[]> inputCoder) {
    return HyperLogLogPlusPlusWrapperCoder.of();
  }

  @Override
  public HyperLogLogPlusPlusWrapper<T> createAccumulator() {
    return new HyperLogLogPlusPlusWrapper<>(null);
  }

  @Override
  public HyperLogLogPlusPlusWrapper<T> addInput(
      HyperLogLogPlusPlusWrapper<T> accumulator, byte[] input) {
    if (accumulator.hll == null) {
      @SuppressWarnings("unchecked")
      HyperLogLogPlusPlus<T> hll = (HyperLogLogPlusPlus<T>) HyperLogLogPlusPlus.forProto(input);
      accumulator.hll = hll;
    } else {
      accumulator.hll.merge(input);
    }
    return accumulator;
  }

  @Override
  public HyperLogLogPlusPlusWrapper<T> mergeAccumulators(
      Iterable<HyperLogLogPlusPlusWrapper<T>> accumulators) {
    HyperLogLogPlusPlusWrapper<T> merged = createAccumulator();
    for (HyperLogLogPlusPlusWrapper<T> accumulator : accumulators) {
      if (accumulator.hll == null) {
        continue;
      }
      if (merged.hll == null) {
        // Cannot set merged.hll to accumulator.hll directly because we shouldn't mutate accumulator
        @SuppressWarnings("unchecked")
        HyperLogLogPlusPlus<T> hll =
            (HyperLogLogPlusPlus<T>)
                HyperLogLogPlusPlus.forProto(accumulator.hll.serializeToProto());
        merged.hll = hll;
      } else {
        merged.hll.merge(accumulator.hll);
      }
    }
    return merged;
  }

  @Override
  public byte[] extractOutput(HyperLogLogPlusPlusWrapper<T> accumulator) {
    if (accumulator.hll == null) {
      throw new IllegalStateException(
          "HllCountMergePartialFn.extractOutput() should not be called on a dummy accumulator.");
    }
    return accumulator.hll.serializeToByteArray();
  }
}
