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
package org.apache.beam.runners.direct;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Aggregator.AggregatorFactory;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

/**
 * AccumT container for the current values associated with {@link Aggregator Aggregators}.
 */
public class AggregatorContainer {

  private static class AggregatorInfo<InputT, AccumT, OutputT> implements Aggregator<InputT, OutputT> {
    private final String name;
    private final CombineFn<InputT, AccumT, OutputT> combiner;
    private AccumT accumulator = null;
    private boolean committed = false;

    private AggregatorInfo(String name, CombineFn<InputT, AccumT, OutputT> combiner) {
      this.name = name;
      this.combiner = combiner;
    }

    @Override
    public void addValue(InputT input) {
      Preconditions.checkState(!committed, "Cannot addValue after committing");
      if (accumulator == null) {
        accumulator = combiner.createAccumulator();
      }
      accumulator = combiner.addInput(accumulator, input);
    }

    public OutputT getOutput() {
      return accumulator == null ? null : combiner.extractOutput(accumulator);
    }

    private void merge(AggregatorInfo<?, ?, ?> other) {
      // TODO: Make sure the CombineFn and accumulators are compatible.
      AggregatorInfo<InputT, AccumT, OutputT> otherSafe =
          (AggregatorInfo<InputT, AccumT, OutputT>) other;
      mergeSafe(otherSafe);
    }

    private void mergeSafe(AggregatorInfo<InputT, AccumT, OutputT> other) {
      if (accumulator == null) {
        accumulator = other.accumulator;
      } else if (other.accumulator != null) {
        accumulator = combiner.mergeAccumulators(Arrays.asList(accumulator, other.accumulator));
      }
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public CombineFn<InputT, ?, OutputT> getCombineFn() {
      return combiner;
    }
  }

  private final HashMap<String, AggregatorInfo<?, ?, ?>> accumulators = new HashMap<>();

  private AggregatorContainer() {
  }

  public static AggregatorContainer create() {
    return new AggregatorContainer();
  }

  @Nullable
  <OutputT> OutputT getAggregate(String name) {
    AggregatorInfo<?, ?, OutputT> aggregatorInfo =
        (AggregatorInfo<?, ?, OutputT>) accumulators.get(name);
    return aggregatorInfo == null ? null : aggregatorInfo.getOutput();
  }

  public Mutator createMutator() {
    return new Mutator(this);
  }

  /**
   * AccumT class for mutations to the aggregator values.
   */
  public static class Mutator implements AggregatorFactory {

    private final HashMap<String, AggregatorInfo<?, ?, ?>> accumulatorDeltas = new HashMap<>();
    private final AggregatorContainer container;
    private boolean committed = false;

    private Mutator(AggregatorContainer container) {
      this.container = container;
    }

    public void commit() {
      Preconditions.checkState(!committed, "Should not be already committed");
      committed = true;
      synchronized (container) {
        for (Map.Entry<String, AggregatorInfo<?, ?, ?>> entry : accumulatorDeltas.entrySet()) {
          AggregatorInfo<?, ?, ?> previous = container.accumulators.get(entry.getKey());
          entry.getValue().committed = true;
          if (previous == null) {
            container.accumulators.put(entry.getKey(), entry.getValue());
          } else {
            previous.merge(entry.getValue());
            previous.committed = true;
          }
        }
      }
    }

    @Override
    public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregator(
        String name,
        CombineFn<InputT, AccumT, OutputT> combine) {
      Preconditions.checkState(!committed, "Cannot create aggregators after committing");
      AggregatorInfo<?, ?, ?> aggregatorInfo = accumulatorDeltas.get(name);
      if (aggregatorInfo != null) {
        // TODO: Verify compatibility
        AggregatorInfo<InputT, ?, OutputT> typedAggregatorInfo =
            (AggregatorInfo<InputT, ?, OutputT>) aggregatorInfo;
        return typedAggregatorInfo;
      } else {
        AggregatorInfo<InputT, ?, OutputT> typedAggregatorInfo =
            new AggregatorInfo<>(name, combine);
        accumulatorDeltas.put(name, typedAggregatorInfo);
        return typedAggregatorInfo;
      }
    }
  }
}
