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
import com.google.zetasketch.shaded.com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link HllCount}. */
@RunWith(JUnit4.class)
public class HllCountTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private static final List<Integer> INTS1 = Arrays.asList(1, 2, 3, 3, 1, 4);
  private static final byte[] INTS1_SKETCH;
  private static final Long INTS1_ESTIMATE;

  static {
    HyperLogLogPlusPlus<Integer> hll = new HyperLogLogPlusPlus.Builder().buildForIntegers();
    INTS1.forEach(hll::add);
    INTS1_SKETCH = hll.serializeToByteArray();
    INTS1_ESTIMATE = hll.longResult();
  }

  private static final List<Integer> INTS2 = Arrays.asList(2, 3, 4, 3, 2, 6, 5);
  private static final byte[] INTS2_SKETCH;
  private static final Long INTS2_ESTIMATE;

  static {
    HyperLogLogPlusPlus<Integer> hll = new HyperLogLogPlusPlus.Builder().buildForIntegers();
    INTS2.forEach(hll::add);
    INTS2_SKETCH = hll.serializeToByteArray();
    INTS2_ESTIMATE = hll.longResult();
  }

  private static final byte[] INTS1_INTS2_SKETCH;

  static {
    HyperLogLogPlusPlus<?> hll = HyperLogLogPlusPlus.forProto(INTS1_SKETCH);
    hll.merge(INTS2_SKETCH);
    INTS1_INTS2_SKETCH = hll.serializeToByteArray();
  }

  private static final List<Long> LONGS = Arrays.asList(1L, 2L, 3L, 3L, 1L, 4L);
  private static final byte[] LONGS_SKETCH;

  static {
    HyperLogLogPlusPlus<Long> hll = new HyperLogLogPlusPlus.Builder().buildForLongs();
    LONGS.forEach(hll::add);
    LONGS_SKETCH = hll.serializeToByteArray();
  }

  private static final byte[] LONGS_BYTES_SKETCH;

  static {
    HyperLogLogPlusPlus<ByteString> hll = new HyperLogLogPlusPlus.Builder().buildForBytes();
    try {
      for (Long l : LONGS) {
        hll.add(CoderUtils.encodeToByteArray(VarLongCoder.of(), l));
      }
    } catch (CoderException e) {
      throw new IllegalArgumentException("Serializing test input LONGS failed.", e);
    }
    LONGS_BYTES_SKETCH = hll.serializeToByteArray();
  }

  private static final List<String> STRINGS = Arrays.asList("s1", "s2", "s3", "s3", "s1", "s4");
  private static final byte[] STRINGS_SKETCH;

  static {
    HyperLogLogPlusPlus<String> hll = new HyperLogLogPlusPlus.Builder().buildForStrings();
    STRINGS.forEach(hll::add);
    STRINGS_SKETCH = hll.serializeToByteArray();
  }

  private static final int TEST_PRECISION = 20;
  private static final byte[] STRINGS_SKETCH_TEST_PRECISION;

  static {
    HyperLogLogPlusPlus<String> hll =
        new HyperLogLogPlusPlus.Builder().normalPrecision(TEST_PRECISION).buildForStrings();
    STRINGS.forEach(hll::add);
    STRINGS_SKETCH_TEST_PRECISION = hll.serializeToByteArray();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitIntegerSketchGlobally() {
    PCollection<byte[]> result =
        p.apply(Create.of(INTS1)).apply(HllCount.Init.integerSketch().globally());

    PAssert.that(result).containsInAnyOrder(INTS1_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitLongSketchGlobally() {
    PCollection<byte[]> result =
        p.apply(Create.of(LONGS)).apply(HllCount.Init.longSketch().globally());

    PAssert.that(result).containsInAnyOrder(LONGS_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitStringSketchGlobally() {
    PCollection<byte[]> result =
        p.apply(Create.of(STRINGS)).apply(HllCount.Init.stringSketch().globally());

    PAssert.that(result).containsInAnyOrder(STRINGS_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitStringSketchGlobally_WithPrecision() {
    PCollection<byte[]> result =
        p.apply(Create.of(STRINGS))
            .apply(HllCount.Init.stringSketch().withPrecision(TEST_PRECISION).globally());

    PAssert.that(result).containsInAnyOrder(STRINGS_SKETCH_TEST_PRECISION);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitBytesSketchGlobally() {
    PCollection<byte[]> result =
        p.apply(Create.of(LONGS)).apply(HllCount.Init.<Long>bytesSketch().globally());

    PAssert.that(result).containsInAnyOrder(LONGS_BYTES_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitIntegerSketchPerKey() {
    List<KV<String, Integer>> input = new ArrayList<>();
    INTS1.forEach(i -> input.add(KV.of("k1", i)));
    INTS1.forEach(i -> input.add(KV.of("k2", i)));
    INTS2.forEach(i -> input.add(KV.of("k1", i)));
    PCollection<KV<String, byte[]>> result =
        p.apply(Create.of(input)).apply(HllCount.Init.integerSketch().perKey());

    PAssert.that(result)
        .containsInAnyOrder(
            Arrays.asList(KV.of("k1", INTS1_INTS2_SKETCH), KV.of("k2", INTS1_SKETCH)));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitLongSketchPerKey() {
    List<KV<String, Long>> input = new ArrayList<>();
    LONGS.forEach(l -> input.add(KV.of("k", l)));
    PCollection<KV<String, byte[]>> result =
        p.apply(Create.of(input)).apply(HllCount.Init.longSketch().perKey());

    PAssert.that(result).containsInAnyOrder(Collections.singletonList(KV.of("k", LONGS_SKETCH)));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitStringSketchPerKey() {
    List<KV<String, String>> input = new ArrayList<>();
    STRINGS.forEach(s -> input.add(KV.of("k", s)));
    PCollection<KV<String, byte[]>> result =
        p.apply(Create.of(input)).apply(HllCount.Init.stringSketch().perKey());

    PAssert.that(result).containsInAnyOrder(Collections.singletonList(KV.of("k", STRINGS_SKETCH)));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitStringSketchPerKey_WithPrecision() {
    List<KV<String, String>> input = new ArrayList<>();
    STRINGS.forEach(s -> input.add(KV.of("k", s)));
    PCollection<KV<String, byte[]>> result =
        p.apply(Create.of(input))
            .apply(HllCount.Init.stringSketch().withPrecision(TEST_PRECISION).perKey());

    PAssert.that(result)
        .containsInAnyOrder(Collections.singletonList(KV.of("k", STRINGS_SKETCH_TEST_PRECISION)));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitBytesSketchPerKey() {
    List<KV<String, Long>> input = new ArrayList<>();
    LONGS.forEach(l -> input.add(KV.of("k", l)));
    PCollection<KV<String, byte[]>> result =
        p.apply(Create.of(input)).apply(HllCount.Init.<Long>bytesSketch().perKey());

    PAssert.that(result)
        .containsInAnyOrder(Collections.singletonList(KV.of("k", LONGS_BYTES_SKETCH)));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialGlobally() {
    PCollection<byte[]> result =
        p.apply(Create.of(INTS1_SKETCH, INTS2_SKETCH)).apply(HllCount.MergePartial.globally());

    PAssert.that(result).containsInAnyOrder(INTS1_INTS2_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialGlobally_SingletonInput() {
    PCollection<byte[]> result =
        p.apply(Create.of(LONGS_SKETCH)).apply(HllCount.MergePartial.globally());

    PAssert.that(result).containsInAnyOrder(LONGS_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialGlobally_EmptyInput() {
    PCollection<byte[]> result =
        p.apply(Create.empty(TypeDescriptor.of(byte[].class)))
            .apply(HllCount.MergePartial.globally());

    PAssert.that(result).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialGlobally_IncompatibleSketches() {
    p.apply(Create.of(INTS1_SKETCH, STRINGS_SKETCH)).apply(HllCount.MergePartial.globally());

    thrown.expect(PipelineExecutionException.class);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialPerKey() {
    PCollection<KV<String, byte[]>> result =
        p.apply(
                Create.of(
                    KV.of("k1", INTS1_SKETCH),
                    KV.of("k2", STRINGS_SKETCH),
                    KV.of("k1", INTS2_SKETCH)))
            .apply(HllCount.MergePartial.perKey());

    PAssert.that(result)
        .containsInAnyOrder(
            Arrays.asList(KV.of("k1", INTS1_INTS2_SKETCH), KV.of("k2", STRINGS_SKETCH)));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialPerKey_IncompatibleSketches() {
    p.apply(
            Create.of(
                KV.of("k1", LONGS_SKETCH),
                KV.of("k2", STRINGS_SKETCH),
                KV.of("k1", LONGS_BYTES_SKETCH)))
        .apply(HllCount.MergePartial.perKey());

    thrown.expect(PipelineExecutionException.class);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExtractGlobally() {
    PCollection<Long> result =
        p.apply(Create.of(INTS1_SKETCH, INTS2_SKETCH)).apply(HllCount.Extract.globally());

    PAssert.that(result).containsInAnyOrder(INTS1_ESTIMATE, INTS2_ESTIMATE);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExtractPerKey() {
    PCollection<KV<String, Long>> result =
        p.apply(Create.of(KV.of("k", INTS1_SKETCH), KV.of("k", INTS2_SKETCH)))
            .apply(HllCount.Extract.perKey());

    PAssert.that(result)
        .containsInAnyOrder(Arrays.asList(KV.of("k", INTS1_ESTIMATE), KV.of("k", INTS2_ESTIMATE)));
    p.run();
  }
}
