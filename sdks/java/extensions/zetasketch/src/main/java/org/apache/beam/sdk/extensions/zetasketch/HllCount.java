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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@code PTransform}s to compute HyperLogLogPlusPlus (HLL++) sketches on data streams based on the
 * <a href="https://github.com/google/zetasketch">ZetaSketch</a> implementation.
 *
 * <p>HLL++ is an algorithm implemented by Google that estimates the count of distinct elements in a
 * data stream. HLL++ requires significantly less memory than the linear memory needed for exact
 * computation, at the cost of a small error. Cardinalities of arbitrary breakdowns can be computed
 * using the HLL++ sketch. See this <a
 * href="http://static.googleusercontent.com/media/research.google.com/en/us/pubs/archive/40671.pdf">published
 * paper</a> for details about the algorithm.
 *
 * <p>HLL++ functions are also supported in <a
 * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions">Google Cloud
 * BigQuery</a>. Using the {@code HllCount PTransform}s makes the interoperation with BigQuery
 * easier.
 *
 * <h3>Examples</h3>
 *
 * <h4>Example 1: Create long-type sketch for a {@code PCollection<Long>} and specify precision</h4>
 *
 * <pre>{@code
 * PCollection<Long> input = ...;
 * int p = ...;
 * PCollection<byte[]> sketch = input.apply(HllCount.Init.longSketch().withPrecision(p).globally());
 * }</pre>
 *
 * <h4>Example 2: Create bytes-type sketch for a {@code PCollection<KV<String, byte[]>>}</h4>
 *
 * <pre>{@code
 * PCollection<KV<String, byte[]>> input = ...;
 * PCollection<KV<String, byte[]>> sketch = input.apply(HllCount.Init.bytesSketch().perKey());
 * }</pre>
 *
 * <h4>Example 3: Merge existing sketches in a {@code PCollection<byte[]>} into a new one</h4>
 *
 * <pre>{@code
 * PCollection<byte[]> sketches = ...;
 * PCollection<byte[]> mergedSketch = sketches.apply(HllCount.MergePartial.globally());
 * }</pre>
 *
 * <h4>Example 4: Estimates the count of distinct elements in a {@code PCollection<String>}</h4>
 *
 * <pre>{@code
 * PCollection<String> input = ...;
 * PCollection<Long> countDistinct =
 *     input.apply(HllCount.Init.stringSketch().globally()).apply(HllCount.Extract.globally());
 * }</pre>
 */
@Experimental
public final class HllCount {

  public static final int MINIMUM_PRECISION = HyperLogLogPlusPlus.MINIMUM_PRECISION;
  public static final int MAXIMUM_PRECISION = HyperLogLogPlusPlus.MAXIMUM_PRECISION;
  public static final int DEFAULT_PRECISION = HyperLogLogPlusPlus.DEFAULT_NORMAL_PRECISION;

  // Cannot be instantiated. This class is intended to be a namespace only.
  private HllCount() {}

  /**
   * Provide {@code PTransform}s to aggregate inputs into HLL++ sketches. The four supported input
   * types are {@code Integer}, {@code Long}, {@code String}, and {@code byte[]}.
   *
   * <p>Sketches are represented using the {@code byte[]} type. Sketches of the same type and {@code
   * precision} can be merged into a new sketch using {@link HllCount.MergePartial}. Estimated count
   * of distinct elements can be extracted from sketches using {@link HllCount.Extract}.
   *
   * <p>Correspond to the {@code HLL_COUNT.INIT(input [, precision])} function in <a
   * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions">BigQuery</a>.
   */
  public static final class Init {

    // Cannot be instantiated. This class is intended to be a namespace only.
    private Init() {}

    /**
     * Returns a {@link HllCount.Init.Builder<Integer>} for a {@code HllCount.Init} combining {@code
     * PTransform}. Call {@link Builder#globally()} or {@link Builder#perKey()} on the returning
     * {@link Builder} to finalize the {@code PTransform}.
     *
     * <p>Calling {@link Builder#globally()} returns a {@code PTransform} that takes an input {@code
     * PCollection<Integer>} and returns a {@code PCollection<byte[]>} whose contents is the
     * integer-type HLL++ sketch computed from the elements in the input {@code PCollection}.
     *
     * <p>Calling {@link Builder#perKey()} returns a {@code PTransform} that takes an input {@code
     * PCollection<KV<K, Integer>>} and returns a {@code PCollection<KV<K, byte[]>>} whose contents
     * is the per-key integer-type HLL++ sketch computed from the values matching each key in the
     * input {@code PCollection}.
     *
     * <p>Integer-type sketches cannot be merged with sketches of other types.
     */
    public static Builder<Integer> integerSketch() {
      return new Builder<>(HllCountInitFn.forInteger());
    }

    /**
     * Returns a {@link HllCount.Init.Builder<Long>} for a {@code HllCount.Init} combining {@code
     * PTransform}. Call {@link Builder#globally()} or {@link Builder#perKey()} on the returning
     * {@link Builder} to finalize the {@code PTransform}.
     *
     * <p>Calling {@link Builder#globally()} returns a {@code PTransform} that takes an input {@code
     * PCollection<Long>} and returns a {@code PCollection<byte[]>} whose contents is the long-type
     * HLL++ sketch computed from the elements in the input {@code PCollection}.
     *
     * <p>Calling {@link Builder#perKey()} returns a {@code PTransform} that takes an input {@code
     * PCollection<KV<K, Long>>} and returns a {@code PCollection<KV<K, byte[]>>} whose contents is
     * the per-key long-type HLL++ sketch computed from the values matching each key in the input
     * {@code PCollection}.
     *
     * <p>Long-type sketches cannot be merged with sketches of other types.
     */
    public static Builder<Long> longSketch() {
      return new Builder<>(HllCountInitFn.forLong());
    }

    /**
     * Returns a {@link HllCount.Init.Builder<String>} for a {@code HllCount.Init} combining {@code
     * PTransform}. Call {@link Builder#globally()} or {@link Builder#perKey()} on the returning
     * {@link Builder} to finalize the {@code PTransform}.
     *
     * <p>Calling {@link Builder#globally()} returns a {@code PTransform} that takes an input {@code
     * PCollection<String>} and returns a {@code PCollection<byte[]>} whose contents is the
     * string-type HLL++ sketch computed from the elements in the input {@code PCollection}.
     *
     * <p>Calling {@link Builder#perKey()} returns a {@code PTransform} that takes an input {@code
     * PCollection<KV<K, String>>} and returns a {@code PCollection<KV<K, byte[]>>} whose contents
     * is the per-key string-type HLL++ sketch computed from the values matching each key in the
     * input {@code PCollection}.
     *
     * <p>String-type sketches cannot be merged with sketches of other types.
     */
    public static Builder<String> stringSketch() {
      return new Builder<>(HllCountInitFn.forString());
    }

    /**
     * Returns a {@link HllCount.Init.Builder<byte[]>} for a {@code HllCount.Init} combining {@code
     * PTransform}. Call {@link Builder#globally()} or {@link Builder#perKey()} on the returning
     * {@link Builder} to finalize the {@code PTransform}.
     *
     * <p>Calling {@link Builder#globally()} returns a {@code PTransform} that takes an input {@code
     * PCollection<byte[]>} and returns a {@code PCollection<byte[]>} whose contents is the
     * bytes-type HLL++ sketch computed from the elements in the input {@code PCollection}.
     *
     * <p>Calling {@link Builder#perKey()} returns a {@code PTransform} that takes an input {@code
     * PCollection<KV<K, byte[]>>} and returns a {@code PCollection<KV<K, byte[]>>} whose contents
     * is the per-key bytes-type HLL++ sketch computed from the values matching each key in the
     * input {@code PCollection}.
     *
     * <p>Bytes-type sketches cannot be merged with sketches of other types.
     */
    public static Builder<byte[]> bytesSketch() {
      return new Builder<>(HllCountInitFn.forBytes());
    }

    /**
     * Builder for the {@code HllCount.Init} combining {@code PTransform}.
     *
     * <p>Call {@link #withPrecision(int)} to customize the {@code precision} parameter of the
     * sketch.
     *
     * <p>Call {@link #globally()} or {@link #perKey()} to finalize the {@code PTransform}.
     *
     * @param <InputT> element type or value type in {@code KV}s of the input {@code PCollection} to
     *     the {@code PTransform} being built
     */
    public static final class Builder<InputT> {

      private final HllCountInitFn<InputT, ?> initFn;

      private Builder(HllCountInitFn<InputT, ?> initFn) {
        this.initFn = initFn;
      }

      /**
       * Explicitly set the {@code precision} parameter used to compute HLL++ sketch.
       *
       * <p>Valid range is between {@value #MINIMUM_PRECISION} and {@value #MAXIMUM_PRECISION}. If
       * this method is not called, {@value #DEFAULT_PRECISION} will be used. Sketches computed
       * using different {@code precision}s cannot be merged together.
       *
       * @param precision the {@code precision} parameter used to compute HLL++ sketch
       */
      public Builder<InputT> withPrecision(int precision) {
        initFn.setPrecision(precision);
        return this;
      }

      /**
       * Returns a {@code PTransform} that takes an input {@code PCollection<InputT>} and returns a
       * {@code PCollection<byte[]>} whose contents is the HLL++ sketch computed from the elements
       * in the input {@code PCollection}.
       */
      public PTransform<PCollection<InputT>, PCollection<byte[]>> globally() {
        return new Globally<>(initFn);
      }

      /**
       * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, InputT>>} and
       * returns a {@code PCollection<KV<K, byte[]>>} whose contents is the per-key HLL++ sketch
       * computed from the values matching each key in the input {@code PCollection}.
       */
      public <K> PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, byte[]>>> perKey() {
        return new PerKey<>(initFn);
      }
    }

    private static final class Globally<InputT>
        extends PTransform<PCollection<InputT>, PCollection<byte[]>> {

      private final HllCountInitFn<InputT, ?> initFn;

      private Globally(HllCountInitFn<InputT, ?> initFn) {
        this.initFn = initFn;
      }

      @Override
      public PCollection<byte[]> expand(PCollection<InputT> input) {
        return input.apply("HllCount.Init.Globally", Combine.globally(initFn));
      }
    }

    private static final class PerKey<K, V>
        extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, byte[]>>> {

      private final HllCountInitFn<V, ?> initFn;

      private PerKey(HllCountInitFn<V, ?> initFn) {
        this.initFn = initFn;
      }

      @Override
      public PCollection<KV<K, byte[]>> expand(PCollection<KV<K, V>> input) {
        return input.apply("HllCount.Init.PerKey", Combine.perKey(initFn));
      }
    }
  }

  /**
   * Provide {@code PTransform}s to merge HLL++ sketches into a new sketch.
   *
   * <p>Only sketches of the same type and {@code precision} can be merged together. If incompatible
   * sketches are provided, a runtime error will occur.
   *
   * <p>Correspond to the {@code HLL_COUNT.MERGE_PARTIAL(sketch)} function in <a
   * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions">BigQuery</a>.
   */
  public static final class MergePartial {

    // Cannot be instantiated. This class is intended to be a namespace only.
    private MergePartial() {}

    /**
     * Returns a {@code PTransform} that takes an input {@code PCollection<byte[]>} of HLL++
     * sketches and returns a {@code PCollection<byte[]>} of a new sketch merged from the input
     * sketches.
     *
     * <p>Only sketches of the same type and {@code precision} can be merged together. If
     * incompatible sketches are provided, a runtime error will occur.
     */
    public static PTransform<PCollection<byte[]>, PCollection<byte[]>> globally() {
      return new Globally();
    }

    /**
     * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, byte[]>>} of (key,
     * HLL++ sketch) pairs and returns a {@code PCollection<KV<K, byte[]>>} of (key, new sketch
     * merged from the input sketches under the key).
     *
     * <p>Only sketches of the same type and {@code precision} can be merged together. If
     * incompatible sketches are provided, a runtime error will occur.
     */
    public static <K> PTransform<PCollection<KV<K, byte[]>>, PCollection<KV<K, byte[]>>> perKey() {
      return new PerKey<>();
    }

    private static final class Globally
        extends PTransform<PCollection<byte[]>, PCollection<byte[]>> {
      @Override
      public PCollection<byte[]> expand(PCollection<byte[]> input) {
        return input.apply(
            "HllCount.MergePartial.Globally",
            Combine.globally(HllCountMergePartialFn.create()).withoutDefaults());
      }
    }

    private static final class PerKey<K>
        extends PTransform<PCollection<KV<K, byte[]>>, PCollection<KV<K, byte[]>>> {
      @Override
      public PCollection<KV<K, byte[]>> expand(PCollection<KV<K, byte[]>> input) {
        return input.apply(
            "HllCount.MergePartial.PerKey", Combine.perKey(HllCountMergePartialFn.create()));
      }
    }
  }

  /**
   * Provide {@code PTransform}s to extract the estimated count of distinct elements (as {@code
   * Long}s) from each HLL++ sketch.
   *
   * <p>Correspond to the {@code HLL_COUNT.EXTRACT(sketch)} function in <a
   * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions">BigQuery</a>.
   */
  public static final class Extract {

    // Cannot be instantiated. This class is intended to be a namespace only.
    private Extract() {}

    /**
     * Returns a {@code PTransform} that takes an input {@code PCollection<byte[]>} of HLL++
     * sketches and returns a {@code PCollection<Long>} of the estimated count of distinct elements
     * extracted from each sketch.
     */
    public static PTransform<PCollection<byte[]>, PCollection<Long>> globally() {
      return new Globally();
    }

    /**
     * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, byte[]>>} of (key,
     * HLL++ sketch) pairs and returns a {@code PCollection<KV<K, Long>>} of (key, estimated count
     * of distinct elements extracted from each sketch).
     */
    public static <K> PTransform<PCollection<KV<K, byte[]>>, PCollection<KV<K, Long>>> perKey() {
      return new PerKey<K>();
    }

    private static final class Globally extends PTransform<PCollection<byte[]>, PCollection<Long>> {
      @Override
      public PCollection<Long> expand(PCollection<byte[]> input) {
        return input.apply(
            "HllCount.Extract.Globally",
            ParDo.of(
                new DoFn<byte[], Long>() {
                  @ProcessElement
                  public void processElement(
                      @Element byte[] sketch, OutputReceiver<Long> receiver) {
                    receiver.output(HyperLogLogPlusPlus.forProto(sketch).result());
                  }
                }));
      }
    }

    private static final class PerKey<K>
        extends PTransform<PCollection<KV<K, byte[]>>, PCollection<KV<K, Long>>> {
      @Override
      public PCollection<KV<K, Long>> expand(PCollection<KV<K, byte[]>> input) {
        return input.apply(
            "HllCount.Extract.PerKey",
            ParDo.of(
                new DoFn<KV<K, byte[]>, KV<K, Long>>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<K, byte[]> kv, OutputReceiver<KV<K, Long>> receiver) {
                    receiver.output(
                        KV.of(kv.getKey(), HyperLogLogPlusPlus.forProto(kv.getValue()).result()));
                  }
                }));
      }
    }
  }
}
