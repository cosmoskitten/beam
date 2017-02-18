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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.io.Write.Bound;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;

/**
 * A {@link PTransformOverrideFactory} that overrides {@link Write} {@link PTransform PTransforms}
 * with an unspecified number of shards with a write with a specified number of shards. The number
 * of shards is the log base 10 of the number of input records, with up to 2 additional shards.
 */
class WriteWithShardingFactory<InputT>
    implements org.apache.beam.sdk.runners.PTransformOverrideFactory<
        PCollection<InputT>, PDone, Write.Bound<InputT>> {
  static final int MAX_RANDOM_EXTRA_SHARDS = 3;
  @VisibleForTesting static final int MIN_SHARDS_FOR_LOG = 3;

  @Override
  public PTransform<PCollection<InputT>, PDone> getReplacementTransform(Bound<InputT> transform) {
    if (transform.getSharding() == null) {
      return transform.withSharding(new LogElementShardsWithDrift<InputT>());
    }
    return transform;
  }

  @Override
  public PCollection<InputT> getInput(List<TaggedPValue> inputs, Pipeline p) {
    return (PCollection<InputT>) Iterables.getOnlyElement(inputs).getValue();
  }

  @Override
  public Map<PValue, ReplacementOutput> mapOutputs(List<TaggedPValue> outputs, PDone newOutput) {
    return Collections.emptyMap();
  }

  private static class LogElementShardsWithDrift<T>
      extends PTransform<PCollection<T>, PCollectionView<Integer>> {

    @Override
    public PCollectionView<Integer> expand(PCollection<T> records) {
      final int randomExtraShards = ThreadLocalRandom.current().nextInt(0, 3);
      return records
          .apply("CountRecords", Count.<T>globally())
          .apply("GenerateShardCount", ParDo.of(new CalculateShardsFn(randomExtraShards)))
          .apply(View.<Integer>asSingleton());
    }
  }

  @VisibleForTesting
  static class CalculateShardsFn extends DoFn<Long, Integer> {
    private final int randomExtraShards;

    @VisibleForTesting
    CalculateShardsFn(int randomExtraShards) {
      this.randomExtraShards = randomExtraShards;
    }

    @ProcessElement
    public void process(ProcessContext ctxt) {
      ctxt.output(calculateShards(ctxt.element()));
    }

    private int calculateShards(long totalRecords) {
      checkArgument(
          totalRecords > 0,
          "KeyBasedOnCountFn cannot be invoked on an element if there are no elements");
      if (totalRecords < MIN_SHARDS_FOR_LOG + randomExtraShards) {
        return (int) totalRecords;
      }
      // 100mil records before >7 output files
      int floorLogRecs = Double.valueOf(Math.log10(totalRecords)).intValue();
      return Math.max(floorLogRecs, MIN_SHARDS_FOR_LOG) + randomExtraShards;
    }
  }
}
