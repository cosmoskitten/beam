/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.math.BigInteger;
import java.util.List;
import org.junit.Test;

/**
 * Tests on {@link SplitGenerator}.
 */
public final class SplitGeneratorTest {

  @Test
  public void testGenerateSegments() throws Exception {

    List<BigInteger> tokens = Lists.transform(Lists.newArrayList(
        "0",
        "1",
        "56713727820156410577229101238628035242",
        "56713727820156410577229101238628035243",
        "113427455640312821154458202477256070484",
        "113427455640312821154458202477256070485"),
        (String string) -> new BigInteger(string));

    SplitGenerator generator = new SplitGenerator("foo.bar.RandomPartitioner");
    List<RingRange> segments = generator.generateSplits(10, tokens);
    assertEquals(15, segments.size());

    assertEquals(
        "(0,1]",
        segments.get(0).toString());

    assertEquals(
        "(56713727820156410577229101238628035242,56713727820156410577229101238628035243]",
        segments.get(5).toString());

    assertEquals(
        "(113427455640312821154458202477256070484,113427455640312821154458202477256070485]",
        segments.get(10).toString());

    tokens = Lists.transform(Lists.newArrayList(
        "5",
        "6",
        "56713727820156410577229101238628035242",
        "56713727820156410577229101238628035243",
        "113427455640312821154458202477256070484",
        "113427455640312821154458202477256070485"),
        (String string) -> new BigInteger(string));

    segments = generator.generateSplits(10, tokens);
    assertEquals(15, segments.size());

    assertEquals(
        "(5,6]",
        segments.get(0).toString());

    assertEquals(
        "(56713727820156410577229101238628035242,56713727820156410577229101238628035243]",
        segments.get(5).toString());

    assertEquals(
        "(113427455640312821154458202477256070484,113427455640312821154458202477256070485]",
        segments.get(10).toString());
  }

  @Test(expected = RuntimeException.class)
  public void testZeroSizeRange() throws Exception {

    List<String> tokenStrings = Lists.newArrayList(
        "0",
        "1",
        "56713727820156410577229101238628035242",
        "56713727820156410577229101238628035242",
        "113427455640312821154458202477256070484",
        "113427455640312821154458202477256070485");

    List<BigInteger> tokens = Lists.transform(tokenStrings,
        (String string) -> new BigInteger(string));

    SplitGenerator generator = new SplitGenerator("foo.bar.RandomPartitioner");
    generator.generateSplits(10, tokens);
  }

  @Test
  public void testRotatedRing() throws Exception {
    List<String> tokenStrings = Lists.newArrayList(
        "56713727820156410577229101238628035243",
        "113427455640312821154458202477256070484",
        "113427455640312821154458202477256070485",
        "5",
        "6",
        "56713727820156410577229101238628035242");

    List<BigInteger> tokens = Lists.transform(tokenStrings,
        (String string) -> new BigInteger(string));

    SplitGenerator generator = new SplitGenerator("foo.bar.RandomPartitioner");
    List<RingRange> segments = generator.generateSplits(10, tokens);
    assertEquals(15, segments.size());

    assertEquals(
        "(113427455640312821154458202477256070484,113427455640312821154458202477256070485]",
        segments.get(4).toString());

    assertEquals(

        "(5,6]",
        segments.get(9).toString());

    assertEquals(
        "(56713727820156410577229101238628035242,56713727820156410577229101238628035243]",
        segments.get(14).toString());
  }

  @Test(expected = RuntimeException.class)
  public void testDisorderedRing() throws Exception {

    List<String> tokenStrings = Lists.newArrayList(
        "0",
        "113427455640312821154458202477256070485",
        "1",
        "56713727820156410577229101238628035242",
        "56713727820156410577229101238628035243",
        "113427455640312821154458202477256070484");

    List<BigInteger> tokens = Lists.transform(tokenStrings,
        (String string) -> new BigInteger(string));

    SplitGenerator generator = new SplitGenerator("foo.bar.RandomPartitioner");
    generator.generateSplits(10, tokens);
    // Will throw an exception when concluding that the repair segments don't add up.
    // This is because the tokens were supplied out of order.
  }

  @Test
  public void testMax() throws Exception {
    BigInteger one = BigInteger.ONE;
    BigInteger ten = BigInteger.TEN;
    assertEquals(ten, SplitGenerator.max(one, ten));
    assertEquals(ten, SplitGenerator.max(ten, one));
    assertEquals(one, SplitGenerator.max(one, one));
    BigInteger minusTen = BigInteger.TEN.negate();
    assertEquals(one, SplitGenerator.max(one, minusTen));
  }

  @Test
  public void testMin() throws Exception {
    BigInteger one = BigInteger.ONE;
    BigInteger ten = BigInteger.TEN;
    assertEquals(one, SplitGenerator.min(one, ten));
    assertEquals(one, SplitGenerator.min(ten, one));
    assertEquals(one, SplitGenerator.min(one, one));
    BigInteger minusTen = BigInteger.TEN.negate();
    assertEquals(minusTen, SplitGenerator.min(one, minusTen));
  }

  @Test
  public void testLowerThan() throws Exception {
    BigInteger one = BigInteger.ONE;
    BigInteger ten = BigInteger.TEN;
    assertTrue(SplitGenerator.lowerThan(one, ten));
    assertFalse(SplitGenerator.lowerThan(ten, one));
    assertFalse(SplitGenerator.lowerThan(ten, ten));
    BigInteger minusTen = BigInteger.TEN.negate();
    assertTrue(SplitGenerator.lowerThan(minusTen, one));
    assertFalse(SplitGenerator.lowerThan(one, minusTen));
  }

  @Test
  public void testGreaterThan() throws Exception {
    BigInteger one = BigInteger.ONE;
    BigInteger ten = BigInteger.TEN;
    assertTrue(SplitGenerator.greaterThan(ten, one));
    assertFalse(SplitGenerator.greaterThan(one, ten));
    assertFalse(SplitGenerator.greaterThan(one, one));
    BigInteger minusTen = BigInteger.TEN.negate();
    assertFalse(SplitGenerator.greaterThan(minusTen, one));
    assertTrue(SplitGenerator.greaterThan(one, minusTen));
  }
}

