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

package org.apache.beam.sdk.testing;

import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

/**
 * Utilities for testing CombineFns, ensuring they give correct results
 * across various permutations and shardings of the input.
 */
public class CombineFnTester {
  public static <InputT, OutputT> void testCombine(
      CombineFn<InputT, ?, OutputT> combineFn, OutputT expected, InputT... inputs) {
    testCombine(combineFn, expected, Lists.newArrayList(inputs));
  }

  public static <InputT, OutputT> void testCombine(
      CombineFn<InputT, ?, OutputT> combineFn, OutputT expected, Iterable<InputT> inputs) {
    checkCombineFn(combineFn, Lists.newArrayList(inputs), expected);
  }

  // //////////////////////////////////////////////////////////////////////////
  private static <InputT, AccumT, OutputT> void checkCombineFn(
      CombineFn<InputT, AccumT, OutputT> fn, List<InputT> input, final OutputT expected) {
    checkCombineFn(fn, input, Matchers.is(expected));
  }

  private static <InputT, AccumT, OutputT> void checkCombineFn(
      CombineFn<InputT, AccumT, OutputT> fn, List<InputT> input, Matcher<? super OutputT> matcher) {
    checkCombineFnInternal(fn, input, matcher);
    Collections.shuffle(input);
    checkCombineFnInternal(fn, input, matcher);
  }

  private static <InputT, AccumT, OutputT> void checkCombineFnInternal(
      CombineFn<InputT, AccumT, OutputT> fn, List<InputT> input, Matcher<? super OutputT> matcher) {
    int size = input.size();
    checkCombineFnShards(fn, Collections.singletonList(input), matcher);
    checkCombineFnShards(fn, shardEvenly(input, 2), matcher);
    if (size > 4) {
      checkCombineFnShards(fn, shardEvenly(input, size / 2), matcher);
      checkCombineFnShards(fn, shardEvenly(input, (int) (size / Math.sqrt(size))), matcher);
    }
    checkCombineFnShards(fn, shardExponentially(input, 1.4), matcher);
    checkCombineFnShards(fn, shardExponentially(input, 2), matcher);
    checkCombineFnShards(fn, shardExponentially(input, Math.E), matcher);
  }

  private static <InputT, AccumT, OutputT> void checkCombineFnShards(
      CombineFn<InputT, AccumT, OutputT> fn,
      List<? extends Iterable<InputT>> shards,
      Matcher<? super OutputT> matcher) {
    checkCombineFnShardsInternal(fn, shards, matcher);
    Collections.shuffle(shards);
    checkCombineFnShardsInternal(fn, shards, matcher);
  }

  private static <InputT, AccumT, OutputT> void checkCombineFnShardsInternal(
      CombineFn<InputT, AccumT, OutputT> fn,
      Iterable<? extends Iterable<InputT>> shards,
      Matcher<? super OutputT> matcher) {
    List<AccumT> accumulators = new ArrayList<>();
    int maybeCompact = 0;
    for (Iterable<InputT> shard : shards) {
      AccumT accumulator = fn.createAccumulator();
      for (InputT elem : shard) {
        accumulator = fn.addInput(accumulator, elem);
      }
      if (maybeCompact++ % 2 == 0) {
        accumulator = fn.compact(accumulator);
      }
      accumulators.add(accumulator);
    }
    AccumT merged = fn.mergeAccumulators(accumulators);
    assertThat(fn.extractOutput(merged), matcher);
  }

  private static <T> List<List<T>> shardEvenly(List<T> input, int numShards) {
    List<List<T>> shards = new ArrayList<>(numShards);
    for (int i = 0; i < numShards; i++) {
      shards.add(input.subList(i * input.size() / numShards, (i + 1) * input.size() / numShards));
    }
    return shards;
  }

  private static <T> List<List<T>> shardExponentially(List<T> input, double base) {
    assert base > 1.0;
    List<List<T>> shards = new ArrayList<>();
    int end = input.size();
    while (end > 0) {
      int start = (int) (end / base);
      shards.add(input.subList(start, end));
      end = start;
    }
    return shards;
  }
}
