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

import java.net.URI;
import org.junit.Test;

/**
 * Unit tests for {@link PathUtils}.
 */
public class PathUtilsTest {
  @Test
  public void testResolveSinglePath() throws Exception {
    assertEquals(
        "/root/tmp/aa",
        PathUtils.resolveAgainstDirectory("/root/tmp", "aa"));
  }

  @Test
  public void testResolveMultiplePaths() throws Exception {
    assertEquals(
        "/root/tmp/aa/bb/cc",
        PathUtils.resolveAgainstDirectory("/root/tmp", "aa", "bb", "cc"));
  }

  @Test
  public void testResolveWithScheme() throws Exception {
    assertEquals(
        "file:/root/tmp/aa",
        PathUtils.resolveAgainstDirectory("file:/root/tmp", "aa"));
    assertEquals(
        "gs://bucket/tmp/aa",
        PathUtils.resolveAgainstDirectory("gs://bucket/tmp", "aa"));
  }

  @Test
  public void testResolveOtherIsAbsolutePath() throws Exception {
    String expected = "/root/tmp/aa";
    assertEquals(expected, PathUtils.resolveAgainstDirectory(expected, expected));
  }

  @Test
  public void testResolveOtherIsEmptyPath() throws Exception {
    String expected = "/root/tmp/aa";
    assertEquals(expected, PathUtils.resolveAgainstDirectory(expected, "", ""));
  }

  @Test
  public void testGetDirectory() throws Exception {
    assertEquals("", PathUtils.getDirectory(""));
    assertEquals("/", PathUtils.getDirectory("/"));
    assertEquals("/", PathUtils.getDirectory("/a"));
    assertEquals("/a/", PathUtils.getDirectory("/a/"));
    assertEquals("ab/", PathUtils.getDirectory("ab/"));
    assertEquals("/ab/", PathUtils.getDirectory("/ab/c"));
  }

  @Test
  public void testGetFileName() throws Exception {
    assertEquals("", PathUtils.getFileName(""));
    assertEquals("", PathUtils.getFileName("/"));
    assertEquals("", PathUtils.getFileName("//"));
    assertEquals("a", PathUtils.getFileName("/a"));
    assertEquals("a", PathUtils.getFileName("/a/"));
    assertEquals("ab", PathUtils.getFileName("ab/"));
    assertEquals("c", PathUtils.getFileName("/ab/c"));
  }

  @Test
  public void testURI() throws Exception {
    System.out.print(URI.create("gs-s3://abc/dfd/.././dfd/..").normalize().toString());
  }
}
