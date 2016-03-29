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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.Partition;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PDone;

import java.security.SecureRandom;
import java.util.Random;

/**
 * A write that explicitly controls its number of output shards.
 */
abstract class ShardControlledWrite<InputT>
    extends ForwardingPTransform<PCollection<InputT>, PDone> {
  @Override
  public PDone apply(PCollection<InputT> input) {
    int numShards = getNumShards();
    checkArgument(
        numShards >= 1,
        "%s should only be applied if the output has a controlled number of shards (> 1); got %s",
        ShardControlledWrite.class.getSimpleName(),
        getNumShards());
    PCollectionList<InputT> shards =
        input.apply(
            "PartitionInto" + numShards + "Shards",
            Partition.of(getNumShards(), new RandomPartitionFn<InputT>()));
    for (int i = 0; i < shards.size(); i++) {
      PCollection<InputT> shard = shards.get(i);
      PTransform<? super PCollection<InputT>, PDone> writeShard = getSingleShardTransform(i);
      shard.apply(String.format("%s(Shard:%s)", writeShard.getName(), i), writeShard);
    }
    return PDone.in(input.getPipeline());
  }

  /**
   * Returns the number of shards this {@link PTransform} should write to.
   */
  abstract int getNumShards();

  /**
   * Returns a {@link PTransform} that performs a write to the shard with the specified shard
   * number.
   *
   * <p>This method will be called n times, where n is the value of {@link #getNumShards()}, for
   * shard numbers {@code [0...n)}.
   */
  abstract PTransform<? super PCollection<InputT>, PDone> getSingleShardTransform(int shardNum);

  private static class RandomPartitionFn<T> implements Partition.PartitionFn<T> {
    private Random random;
    @Override
    public int partitionFor(T elem, int numPartitions) {
      if (random == null) {
        // lazily initialize at execution time
        random = new SecureRandom();
      }
      return random.nextInt(numPartitions);
    }
  }
}
