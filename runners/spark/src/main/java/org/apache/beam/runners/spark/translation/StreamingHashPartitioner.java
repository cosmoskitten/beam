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
package org.apache.beam.runners.spark.translation;

import com.google.common.hash.Hashing;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.util.Utils;

/** Replaces the use of Spark's {@link HashPartitioner} due to a bug when the partitioning key is an array. */
public class StreamingHashPartitioner extends Partitioner {

  private int partitions;

  private StreamingHashPartitioner(int partitions) {
    this.partitions = partitions;
  }

  public static StreamingHashPartitioner of(int partitions)  {
    return new StreamingHashPartitioner(partitions);
  }

  @Override
  public int numPartitions() {
    return partitions;
  }

  @Override
  public int getPartition(Object key) {
    /**
     * Java arrays have hashCodes that are based on the arrays' identities rather than their contents.
     * This essentially means that if we have an array as a partition key, we would get incorrect results.
     * In order to avoid getting incorrect results, we use a stable hashing function when the input is
     * an array.
     */
    if (key instanceof byte[]) {
      return Utils.nonNegativeMod((Hashing.murmur3_128().hashCode()), partitions);
    } else {
      return Utils.nonNegativeMod(java.util.Objects.hash(key), partitions);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StreamingHashPartitioner) {
      return ((StreamingHashPartitioner) obj).partitions == partitions;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return partitions;
  }
}
