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

package org.apache.beam.sdk.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.testing.EqualsTester;
import org.junit.Test;

/**
 * Unit tests for {@link FileNameTemplate}.
 */
public class FileNameTemplateTest {


  @Test
  public void testTypicalUsages() {
    FileNameTemplate normalTemplate = FileNameTemplate.of("output", "-SSS-of-NNN", ".txt");
    assertEquals("output-001-of-123.txt", normalTemplate.apply(1, 123));

    FileNameTemplate templateWithSlash = FileNameTemplate.of("out.txt", "/part-SSSSS", "");
    assertEquals("out.txt/part-00042", templateWithSlash.apply(42, 100));

    FileNameTemplate noShardNumTokens = FileNameTemplate.of("ou", "t.t", "xt");
    assertEquals("out.txt", noShardNumTokens.apply(1, 1));

    FileNameTemplate simpleConcat = FileNameTemplate.of("out", "SSNNshard", ".txt");
    assertEquals("out0102shard.txt",
        simpleConcat.apply(1, 2));

    FileNameTemplate specialCharacters = FileNameTemplate.of("out", "-N/S.part-S-of-N", ".txt");
    assertEquals("out-2/1.part-1-of-2.txt", specialCharacters.apply(1, 2));
  }

  @Test
  public void testConstructionValidation() {
    try {
      FileNameTemplate.of(null, "shardTemplate", "suffix");
      fail("Should have thrown; prefix should not be null.");
    } catch (NullPointerException e) {
      // expected
    }

    try {
      FileNameTemplate.of("prefix", null, "suffix");
      fail("Should have thrown; shardTemplate should not be null.");
    } catch (NullPointerException e) {
      // expected
    }

    try {
      FileNameTemplate.of("prefix", "shardTemplate", null);
      fail("Should have thrown; suffix should not be null.");
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testInputShardsRangeValidation() {
    FileNameTemplate template = FileNameTemplate.of("prefix", "shardTemplate", "suffix");

    try {
      template.apply(-1, 100);
      fail("Should have thrown; shardNum must be non-negative.");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }

    try {
      template.apply(0, -1);
      fail("Should have thrown; numShards must be non-negative.");
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      template.apply(2, 1);
      fail("Should have thrown; shardNum must be less than or equal to numShards.");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }

    // Should not throw:
    template.apply(0, 0);
    template.apply(100, 100);
  }

  @Test
  public void testEmptyTemplate() {
    FileNameTemplate template = FileNameTemplate.of("", "", "");
    assertEquals("", template.apply(1, 100));
  }

  @Test
  public void testShardsLargerThanTemplateTemplate() {
    FileNameTemplate template = FileNameTemplate.of("", "S-of-N", "");
    assertEquals("10-of-10", template.apply(10, 10));
  }

  @Test
  public void testDefaultTemplate() {
    FileNameTemplate expected = FileNameTemplate.of("", ShardNameTemplate.INDEX_OF_MAX, "");
    assertEquals(expected, FileNameTemplate.DEFAULT);
  }

  @Test
  public void testWithPrefix() {
    assertEquals(
        FileNameTemplate.of("a", "b", "c"),
        FileNameTemplate.of("", "b", "c").withPrefix("a")
    );
  }

  @Test
  public void testWithShardNameTemplate() {
    assertEquals(
        FileNameTemplate.of("a", "b", "c"),
        FileNameTemplate.of("a", "", "c").withShardTemplate("b")
    );
  }

  @Test
  public void testWithSuffix() {
    assertEquals(
        FileNameTemplate.of("a", "b", "c"),
        FileNameTemplate.of("a", "b", "").withSuffix("c")
    );
  }

  @Test
  public void testEquality() {
    new EqualsTester()
        .addEqualityGroup(FileNameTemplate.of("a", "b", "c"), FileNameTemplate.of("a", "b", "c"))
        .addEqualityGroup(FileNameTemplate.of("a1", "b", "c"))
        .addEqualityGroup(FileNameTemplate.of("a", "b2", "c"))
        .addEqualityGroup(FileNameTemplate.of("a", "b", "c2"))
        .testEquals();
  }

  @Test
  public void testToString() {
    FileNameTemplate template = FileNameTemplate.of("output", "-SSS-of-NNN", ".txt");
    assertEquals("output-SSS-of-NNN.txt", template.toString());
  }
}
