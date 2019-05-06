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
package org.apache.beam.sdk.extensions.smb;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.smb.SMBCoGbkResult.ToFinalResult;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInputs.BucketedInput;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;

/** Abstractions for SMB sink/source creation. */
public class SortedBucketIO {

  /**
   * Pre-built transform for an SortedBucketSource transform with two bucketed inputs.
   *
   * @param <V1>
   * @param <V2>
   */
  static class TwoSourceJoinResult<V1, V2> extends ToFinalResult<KV<Iterable<V1>, Iterable<V2>>> {
    private final TupleTag<V1> lhsTupleTag;
    private final TupleTag<V2> rhsTupleTag;
    private final Coder<V1> lhsCoder; // @Todo: can we get these from Coder registry?
    private final Coder<V2> rhsCoder;

    TwoSourceJoinResult(
        TupleTag<V1> lhsTupleTag,
        TupleTag<V2> rhsTupleTag,
        Coder<V1> lhsCoder,
        Coder<V2> rhsCoder) {
      this.lhsTupleTag = lhsTupleTag;
      this.rhsTupleTag = rhsTupleTag;
      this.lhsCoder = lhsCoder;
      this.rhsCoder = rhsCoder;
    }

    @Override
    public KV<Iterable<V1>, Iterable<V2>> apply(SMBCoGbkResult input) {
      return KV.of(input.getValuesForTag(lhsTupleTag), input.getValuesForTag(rhsTupleTag));
    }

    @Override
    public Coder<KV<Iterable<V1>, Iterable<V2>>> resultCoder() {
      return KvCoder.of(
          NullableCoder.of(IterableCoder.of(lhsCoder)),
          NullableCoder.of(IterableCoder.of(rhsCoder)));
    }
  }

  /**
   * Implements a typed SortedBucketSource for 2 sources.
   *
   * @param <KeyT>
   * @param <V1>
   * @param <V2>
   */
  public static class SortedBucketSourceJoinBuilder<KeyT, V1, V2> {
    private Class<KeyT> keyClass;
    private JoinSource<KeyT, V1> lhs;
    private JoinSource<KeyT, V2> rhs;

    /**
     * Represents a typed input to an SMB join.
     *
     * @param <K>
     * @param <V>
     */
    public static class JoinSource<K, V> {
      private final BucketedInput<K, V> bucketedInput;
      private final Coder<V> valueCoder;

      public JoinSource(BucketedInput<K, V> bucketedInput, Coder<V> valueCoder) {
        this.bucketedInput = bucketedInput;
        this.valueCoder = valueCoder;
      }
    }

    private SortedBucketSourceJoinBuilder(Class<KeyT> keyClass) {
      this.keyClass = keyClass;
    }

    private SortedBucketSourceJoinBuilder(Class<KeyT> keyClass, JoinSource<KeyT, V1> lhs) {
      this(keyClass);
      this.lhs = lhs;
    }

    public static <KeyT> SortedBucketSourceJoinBuilder<KeyT, ?, ?> withFinalKeyType(
        Class<KeyT> keyClass) {
      return new SortedBucketSourceJoinBuilder<>(keyClass);
    }

    public <ValueT> SortedBucketSourceJoinBuilder<KeyT, ValueT, ?> of(
        JoinSource<KeyT, ValueT> lhs) {
      final SortedBucketSourceJoinBuilder<KeyT, ValueT, ?> builderCopy =
          new SortedBucketSourceJoinBuilder<>(keyClass);

      builderCopy.lhs = lhs;
      return builderCopy;
    }

    public <ValueT> SortedBucketSourceJoinBuilder<KeyT, V1, ValueT> and(
        JoinSource<KeyT, ValueT> rhs) {
      final SortedBucketSourceJoinBuilder<KeyT, V1, ValueT> builderCopy =
          new SortedBucketSourceJoinBuilder<>(keyClass, lhs);

      builderCopy.rhs = rhs;
      return builderCopy;
    }

    public SortedBucketSource<KeyT, KV<Iterable<V1>, Iterable<V2>>> build() {
      return new SortedBucketSource<>(
          ImmutableList.of(lhs.bucketedInput, rhs.bucketedInput),
          keyClass,
          new SortedBucketIO.TwoSourceJoinResult<>(
              lhs.bucketedInput.tupleTag, rhs.bucketedInput.tupleTag,
              lhs.valueCoder, rhs.valueCoder));
    }
  }

  // Sinks

  public static <KeyT, ValueT> SortedBucketSink<KeyT, ValueT> sink(
      BucketMetadata<KeyT, ValueT> bucketingMetadata,
      ResourceId outputDirectory,
      String filenameSuffix,
      ResourceId tempDirectory,
      FileOperations<ValueT> fileOperations) {
    return new SortedBucketSink<>(
        bucketingMetadata,
        new SMBFilenamePolicy(outputDirectory, filenameSuffix),
        fileOperations::createWriter,
        tempDirectory);
  }
}