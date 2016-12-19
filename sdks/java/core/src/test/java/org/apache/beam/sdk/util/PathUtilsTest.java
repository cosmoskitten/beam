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
package org.apache.beam.sdk.util;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link PathUtils}.
 */
public class PathUtilsTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testResolveAgainstDirectory() throws Exception {
    // Tests for local files without the scheme.
    assertEquals(
        "/root/tmp/aa",
        PathUtils.resolveAgainstDirectory("/root/tmp", "aa"));
    assertEquals(
        "/root/tmp/aa/bb/cc",
        PathUtils.resolveAgainstDirectory("/root/tmp", "aa", "bb", "cc"));

    // Tests uris with scheme.
    assertEquals(
        "file:/root/tmp/aa",
        PathUtils.resolveAgainstDirectory("file:/root/tmp", "aa"));
    assertEquals(
        "file:/aa",
        PathUtils.resolveAgainstDirectory("file:///", "aa"));
    assertEquals(
        "gs://bucket/tmp/aa",
        PathUtils.resolveAgainstDirectory("gs://bucket/tmp", "aa"));

    // Tests for Windows OS path in URI format.
    assertEquals(
        "file:/C:/home%20dir/a%20b/a%20b",
        PathUtils.resolveAgainstDirectory("file:/C:/home%20dir", "a%20b", "a%20b"));

    // Tests absolute path.
    assertEquals(
        "/root/tmp/aa",
        PathUtils.resolveAgainstDirectory("/root/tmp/aa", "/root/tmp/aa"));

    // Tests authority with empty path.
    assertEquals(
        "gs://bucket/staging",
        PathUtils.resolveAgainstDirectory("gs://bucket/", "staging"));
    assertEquals(
        "gs://bucket/staging",
        PathUtils.resolveAgainstDirectory("gs://bucket", "staging"));
    assertEquals(
        "gs://bucket/",
        PathUtils.resolveAgainstDirectory("gs://bucket", "."));

    // Tests empty authority and path.
    assertEquals(
        "file:/aa",
        PathUtils.resolveAgainstDirectory("file:///", "aa"));

    // Tests query and fragment.
    assertEquals(
        "file:/home/output/aa/bb",
        PathUtils.resolveAgainstDirectory("file:/home/output?query#fragment", "aa", "bb"));

    // Tests normalizing of "." and ".."
    assertEquals(
        "s3://authority/../home/bb",
        PathUtils.resolveAgainstDirectory("s3://authority/../home/output/..", "aa", "..", "bb"));
    assertEquals(
        "s3://authority/aa/bb",
        PathUtils.resolveAgainstDirectory("s3://authority/.", "aa", ".", "bb"));
    assertEquals(
        "aa/bb",
        PathUtils.resolveAgainstDirectory("a/..", "aa", ".", "bb"));
    assertEquals(
        "/aa/bb",
        PathUtils.resolveAgainstDirectory("/a/..", "aa", ".", "bb"));

    // Tests  ".", "./", "..", "../", "~".
    assertEquals(
        "aa/bb",
        PathUtils.resolveAgainstDirectory(".", "aa", "./", "bb"));
    assertEquals(
        "aa/bb",
        PathUtils.resolveAgainstDirectory("./", "aa", "./", "bb"));
    assertEquals(
        "../aa/bb",
        PathUtils.resolveAgainstDirectory("..", "aa", "./", "bb"));
    assertEquals(
        "../aa/bb",
        PathUtils.resolveAgainstDirectory("../", "aa", "./", "bb"));
    assertEquals(
        "~/aa/bb",
        PathUtils.resolveAgainstDirectory("~", "aa", "./", "bb"));
  }

  @Test
  public void testResolveOtherIsEmptyPath() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected other is not empty.");
    // Tests resolving empty strings.
    PathUtils.resolveAgainstDirectory("/root/tmp/aa", "", "");
  }

  @Test
  public void testResolveOtherHasQuery() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected no query in other");
    // Tests resolving empty strings.
    PathUtils.resolveAgainstDirectory("/root/tmp/aa", "bb?q");
  }

  @Test
  public void testResolveOtherHasFragment() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected no fragment in other");
    // Tests resolving empty strings.
    PathUtils.resolveAgainstDirectory("/root/tmp/aa", "bb#q");
  }

  @Test
  public void testResolveWithIllegalChar() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Illegal character");
    // Tests for Windows OS path with unescaped chars in URI format.
    PathUtils.resolveAgainstDirectory("file:/C:/home dir", "a b", "a b");
  }
}
