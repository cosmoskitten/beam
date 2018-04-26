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

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

/** Tests on {@link SplitGenerator}. */
public final class SplitGeneratorTest {

  @Test
  public void testGenerateSegments() {
    List<BigInteger> tokens =
        Arrays.asList(
                "0",
                "1",
                "56713727820156410577229101238628035242",
                "56713727820156410577229101238628035243",
                "113427455640312821154458202477256070484",
                "113427455640312821154458202477256070485")
            .stream()
            .map(s -> new BigInteger(s))
            .collect(Collectors.toList());

    SplitGenerator generator = new SplitGenerator("foo.bar.RandomPartitioner");
    List<RingRange> segments = generator.generateSplits(10, tokens);

    assertEquals(15, segments.size());
    assertEquals("(0,1]", segments.get(0).toString());
    assertEquals(
        "(56713727820156410577229101238628035242,56713727820156410577229101238628035243]",
        segments.get(5).toString());
    assertEquals(
        "(113427455640312821154458202477256070484,113427455640312821154458202477256070485]",
        segments.get(10).toString());

    tokens =
        Arrays.asList(
                "5",
                "6",
                "56713727820156410577229101238628035242",
                "56713727820156410577229101238628035243",
                "113427455640312821154458202477256070484",
                "113427455640312821154458202477256070485")
            .stream()
            .map(s -> new BigInteger(s))
            .collect(Collectors.toList());

    segments = generator.generateSplits(10, tokens);

    assertEquals(15, segments.size());
    assertEquals("(5,6]", segments.get(0).toString());
    assertEquals(
        "(56713727820156410577229101238628035242,56713727820156410577229101238628035243]",
        segments.get(5).toString());
    assertEquals(
        "(113427455640312821154458202477256070484,113427455640312821154458202477256070485]",
        segments.get(10).toString());
  }

  @Test(expected = RuntimeException.class)
  public void testZeroSizeRange() {
    List<String> tokenStrings =
        Arrays.asList(
            "0",
            "1",
            "56713727820156410577229101238628035242",
            "56713727820156410577229101238628035242",
            "113427455640312821154458202477256070484",
            "113427455640312821154458202477256070485");

    List<BigInteger> tokens =
        tokenStrings.stream().map(s -> new BigInteger(s)).collect(Collectors.toList());

    SplitGenerator generator = new SplitGenerator("foo.bar.RandomPartitioner");
    generator.generateSplits(10, tokens);
  }

  @Test
  public void testRotatedRing() {
    List<String> tokenStrings =
        Arrays.asList(
            "56713727820156410577229101238628035243",
            "113427455640312821154458202477256070484",
            "113427455640312821154458202477256070485",
            "5",
            "6",
            "56713727820156410577229101238628035242");

    List<BigInteger> tokens =
        tokenStrings.stream().map(s -> new BigInteger(s)).collect(Collectors.toList());

    SplitGenerator generator = new SplitGenerator("foo.bar.RandomPartitioner");
    List<RingRange> segments = generator.generateSplits(10, tokens);
    assertEquals(15, segments.size());
    assertEquals(
        "(113427455640312821154458202477256070484,113427455640312821154458202477256070485]",
        segments.get(4).toString());
    assertEquals("(5,6]", segments.get(9).toString());
    assertEquals(
        "(56713727820156410577229101238628035242,56713727820156410577229101238628035243]",
        segments.get(14).toString());
  }

  @Test(expected = RuntimeException.class)
  public void testDisorderedRing() {
    List<String> tokenStrings =
        Arrays.asList(
            "0",
            "113427455640312821154458202477256070485",
            "1",
            "56713727820156410577229101238628035242",
            "56713727820156410577229101238628035243",
            "113427455640312821154458202477256070484");

    List<BigInteger> tokens =
        tokenStrings.stream().map(s -> new BigInteger(s)).collect(Collectors.toList());

    SplitGenerator generator = new SplitGenerator("foo.bar.RandomPartitioner");
    generator.generateSplits(10, tokens);
    // Will throw an exception when concluding that the repair segments don't add up.
    // This is because the tokens were supplied out of order.
  }
}
