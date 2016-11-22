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

import java.nio.file.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link PathUtils}.
 */
public class PathUtilsTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testResolveSinglePath() throws Exception {
    String expected = tmpFolder.getRoot().toPath().resolve("aa").toString();
    assertEquals(expected, PathUtils.resolveAgainstDirectory(tmpFolder.getRoot().toString(), "aa"));
  }

  @Test
  public void testResolveMultiplePaths() throws Exception {
    String expected =
        tmpFolder.getRoot().toPath()
            .resolve("aa")
            .resolve("bb")
            .resolve("cc").toString();
    assertEquals(expected,
        PathUtils.resolveAgainstDirectory(tmpFolder.getRoot().getPath(), "aa", "bb", "cc"));
  }


  @Test
  public void testResolveWithScheme() throws Exception {
    Path rootPath = tmpFolder.getRoot().toPath();
    String rootString = rootPath.toString();

    String expected = rootPath.resolve("aa").toString();
    assertEquals(expected, PathUtils.resolveAgainstDirectory(rootString, "aa"));
    assertEquals("file:" + expected, PathUtils.resolveAgainstDirectory("file:" + rootString, "aa"));
    assertEquals("file:" + expected, PathUtils.resolveAgainstDirectory("file://" + rootString, "aa"));
  }

  @Test
  public void testResolveOtherIsFullPath() throws Exception {
    String expected = tmpFolder.getRoot().getPath();
    assertEquals(expected, PathUtils.resolveAgainstDirectory(expected, expected));
  }

  @Test
  public void testResolveOtherIsEmptyPath() throws Exception {
    String expected = tmpFolder.getRoot().getPath();
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
}
