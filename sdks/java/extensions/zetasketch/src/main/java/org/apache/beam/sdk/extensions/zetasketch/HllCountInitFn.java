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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.zetasketch.HyperLogLogPlusPlus;
import com.google.zetasketch.shaded.com.google.protobuf.ByteString;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;

/**
 * {@link Combine.CombineFn} for the {@link HllCount.Init} combiner.
 *
 * @param <InputT> type of input values to the function
 * @param <HllT> type of the HLL++ sketch to compute
 */
abstract class HllCountInitFn<InputT, HllT>
    extends Combine.CombineFn<InputT, HyperLogLogPlusPlus<HllT>, byte[]> {

  private int precision;

  private HllCountInitFn() {
    setPrecision(HllCount.DEFAULT_PRECISION);
  }

  int getPrecision() {
    return precision;
  }

  void setPrecision(int precision) {
    checkArgument(
        precision >= HllCount.MINIMUM_PRECISION && precision <= HllCount.MAXIMUM_PRECISION,
        "Invalid precision: %s. Valid range is [%s, %s].",
        precision,
        HllCount.MINIMUM_PRECISION,
        HllCount.MAXIMUM_PRECISION);
    this.precision = precision;
  }

  @Override
  public Coder<HyperLogLogPlusPlus<HllT>> getAccumulatorCoder(
      CoderRegistry registry, Coder<InputT> inputCoder) {
    return HyperLogLogPlusPlusCoder.of();
  }

  @Override
  public HyperLogLogPlusPlus<HllT> mergeAccumulators(
      Iterable<HyperLogLogPlusPlus<HllT>> accumulators) {
    HyperLogLogPlusPlus<HllT> merged = createAccumulator();
    for (HyperLogLogPlusPlus<HllT> accumulator : accumulators) {
      // TODO: check if the merge function can accept HyperLogLogPlusPlus<?>
      // Type parameters for this class and MergePartialFn could be simpler (not exposing HllT?)
      merged.merge(accumulator);
    }
    return merged;
  }

  @Override
  public byte[] extractOutput(HyperLogLogPlusPlus<HllT> accumulator) {
    return accumulator.serializeToByteArray();
  }

  static HllCountInitFn<Integer, Integer> forInteger() {
    return new ForInteger();
  }

  static HllCountInitFn<Long, Long> forLong() {
    return new ForLong();
  }

  static HllCountInitFn<String, String> forString() {
    return new ForString();
  }

  static HllCountInitFn<byte[], ByteString> forBytes() {
    return new ForBytes();
  }

  private static class ForInteger extends HllCountInitFn<Integer, Integer> {

    @Override
    public HyperLogLogPlusPlus<Integer> createAccumulator() {
      // TODO: check BigQuery's sparsePrecision (customized, default, or disabled), same below * 3
      // TODO: check BigQuery's INT64/STRING/BYTES type's compatibility with Beam, same below * 3
      return new HyperLogLogPlusPlus.Builder().normalPrecision(getPrecision()).buildForIntegers();
    }

    @Override
    public HyperLogLogPlusPlus<Integer> addInput(
        HyperLogLogPlusPlus<Integer> accumulator, Integer input) {
      accumulator.add(input.intValue());
      return accumulator;
    }
  }

  private static class ForLong extends HllCountInitFn<Long, Long> {

    @Override
    public HyperLogLogPlusPlus<Long> createAccumulator() {
      return new HyperLogLogPlusPlus.Builder().normalPrecision(getPrecision()).buildForLongs();
    }

    @Override
    public HyperLogLogPlusPlus<Long> addInput(HyperLogLogPlusPlus<Long> accumulator, Long input) {
      accumulator.add(input.longValue());
      return accumulator;
    }
  }

  private static class ForString extends HllCountInitFn<String, String> {

    @Override
    public HyperLogLogPlusPlus<String> createAccumulator() {
      return new HyperLogLogPlusPlus.Builder().normalPrecision(getPrecision()).buildForStrings();
    }

    @Override
    public HyperLogLogPlusPlus<String> addInput(
        HyperLogLogPlusPlus<String> accumulator, String input) {
      accumulator.add(input);
      return accumulator;
    }
  }

  private static class ForBytes extends HllCountInitFn<byte[], ByteString> {

    @Override
    public HyperLogLogPlusPlus<ByteString> createAccumulator() {
      return new HyperLogLogPlusPlus.Builder().normalPrecision(getPrecision()).buildForBytes();
    }

    @Override
    public HyperLogLogPlusPlus<ByteString> addInput(
        HyperLogLogPlusPlus<ByteString> accumulator, byte[] input) {
      accumulator.add(input);
      return accumulator;
    }
  }
}
