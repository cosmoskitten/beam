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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.net.URI;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link FileSystems}.
 */
@RunWith(JUnit4.class)
public class FileSystemsTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSpecToURIWindowsOS() throws Exception {
    if (SystemUtils.IS_OS_WINDOWS) {
      assertEquals(
          "file:///C:/home",
          FileSystems.specToURI("C:\\home").toString());

      assertEquals(
          "file:///C:/home/*",
          FileSystems.specToURI("C:\\home\\*").toString());
    } else {
      // Skip tests
    }
  }

  @Test
  public void testSpecToURI() throws Exception {
    if (!SystemUtils.IS_OS_WINDOWS) {
      // Test path without scheme.
      assertEquals(
          "/home/output",
          FileSystems.specToURI("/home/output").toString());
      // Test path with scheme.
      assertEquals(
          "file:///home/output",
          FileSystems.specToURI("file:///home/output").toString());
      assertEquals(
          "file:/C:/home%20dir/a%20b.txt",
          FileSystems.specToURI("file:/C:/home dir/a b.txt").toString());
      // Test asterisks.
      assertEquals(
          "/home/output/**/*",
          FileSystems.specToURI("/home/output/**/*").toString());
      // Test escaped chars.
      assertEquals(
          "/home/out%20put/*",
          FileSystems.specToURI("/home/out put/*").toString());
    } else {
      // Skip tests
    }
  }

  @Test
  public void testSpecToURINonLocalFileSystem() throws Exception {
    assertEquals(
        "gs://my%20bucket/output",
        FileSystems.specToURI("gs://my bucket/output").toString());
    assertEquals(
        "s3://my%20bucket/output/*",
        FileSystems.specToURI("s3://my bucket/output/*").toString());
  }

  @Test
  public void testResolveAgainstDirectory() throws Exception {
    // Tests for local files without the scheme.
    assertEquals(
        "/root/tmp/aa",
        FileSystems.resolveAgainstDirectory("/root/tmp", "aa"));
    assertEquals(
        "/root/tmp/aa/bb/cc",
        FileSystems.resolveAgainstDirectory("/root/tmp", "aa", "bb", "cc"));

    // Tests uris with scheme.
    assertEquals(
        "file:/root/tmp/aa",
        FileSystems.resolveAgainstDirectory("file:/root/tmp", "aa"));
    assertEquals(
        "file:/aa",
        FileSystems.resolveAgainstDirectory("file:///", "aa"));
    assertEquals(
        "gs://bucket/tmp/aa",
        FileSystems.resolveAgainstDirectory("gs://bucket/tmp", "aa"));

    // Tests for Windows OS path in URI format.
    assertEquals(
        "file:/C:/home%20dir/a%20b/a%20b",
        FileSystems.resolveAgainstDirectory("file:/C:/home%20dir", "a%20b", "a%20b"));

    // Tests absolute path.
    assertEquals(
        "/root/tmp/aa",
        FileSystems.resolveAgainstDirectory("/root/tmp/bb", "/root/tmp/aa"));

    // Tests authority with empty path.
    assertEquals(
        "gs://bucket/staging",
        FileSystems.resolveAgainstDirectory("gs://bucket/", "staging"));
    assertEquals(
        "gs://bucket/staging",
        FileSystems.resolveAgainstDirectory("gs://bucket", "staging"));
    assertEquals(
        "gs://bucket/",
        FileSystems.resolveAgainstDirectory("gs://bucket", "."));

    // Tests empty authority and path.
    assertEquals(
        "file:/aa",
        FileSystems.resolveAgainstDirectory("file:///", "aa"));

    // Tests normalizing of "." and ".."
    assertEquals(
        "s3://authority/../home/bb",
        FileSystems.resolveAgainstDirectory("s3://authority/../home/output/..", "aa", "..", "bb"));
    assertEquals(
        "s3://authority/aa/bb",
        FileSystems.resolveAgainstDirectory("s3://authority/.", "aa", ".", "bb"));
    assertEquals(
        "aa/bb",
        FileSystems.resolveAgainstDirectory("a/..", "aa", ".", "bb"));
    assertEquals(
        "/aa/bb",
        FileSystems.resolveAgainstDirectory("/a/..", "aa", ".", "bb"));

    // Tests  ".", "./", "..", "../", "~".
    assertEquals(
        "aa/bb",
        FileSystems.resolveAgainstDirectory(".", "aa", "./", "bb"));
    assertEquals(
        "aa/bb",
        FileSystems.resolveAgainstDirectory("./", "aa", "./", "bb"));
    assertEquals(
        "../aa/bb",
        FileSystems.resolveAgainstDirectory("..", "aa", "./", "bb"));
    assertEquals(
        "../aa/bb",
        FileSystems.resolveAgainstDirectory("../", "aa", "./", "bb"));
    assertEquals(
        "~/aa/bb",
        FileSystems.resolveAgainstDirectory("~", "aa", "./", "bb"));
  }

  @Test
  public void testResolveOtherIsEmptyPath() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected other is not empty.");
    // Tests resolving empty strings.
    FileSystems.resolveAgainstDirectory("/root/tmp/aa", "", "");
  }

  @Test
  public void testResolveDirectoryHasQuery() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected no query in directory");
    // Tests resolving empty strings.
    FileSystems.resolveAgainstDirectory("/root/tmp/aa?q", "bb");
  }

  @Test
  public void testResolveDirectoryHasFragment() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected no fragment in directory");
    // Tests resolving empty strings.
    FileSystems.resolveAgainstDirectory("/root/tmp/aa#q", "bb");
  }

  @Test
  public void testResolveOtherHasQuery() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected no query in other");
    // Tests resolving empty strings.
    FileSystems.resolveAgainstDirectory("/root/tmp/aa", "bb?q");
  }

  @Test
  public void testResolveOtherHasFragment() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected no fragment in other");
    // Tests resolving empty strings.
    FileSystems.resolveAgainstDirectory("/root/tmp/aa", "bb#q");
  }

  @Test
  public void testResolveWithIllegalChar() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Illegal character");
    // Tests for Windows OS path with unescaped chars in URI format.
    FileSystems.resolveAgainstDirectory("file:/C:/home dir", "a b", "a b");
  }

  @Test
  public void testGetLowerCaseScheme() throws Exception {
    assertEquals("gs", FileSystems.getLowerCaseScheme("gs://bucket/output"));
    assertEquals("gs", FileSystems.getLowerCaseScheme("GS://bucket/output"));
    assertEquals("file", FileSystems.getLowerCaseScheme("/home/output"));
    assertEquals("file", FileSystems.getLowerCaseScheme("file:///home/output"));
    assertEquals("file", FileSystems.getLowerCaseScheme("C:\\home\\output"));
    assertEquals("file", FileSystems.getLowerCaseScheme("C:\\home\\output"));
  }

  @Test
  public void testSetDefaultConfig() throws Exception {
    PipelineOptions first = PipelineOptionsFactory.create();
    PipelineOptions second = PipelineOptionsFactory.create();
    FileSystems.setDefaultConfig("file", first);
    assertEquals(first, FileSystems.getDefaultConfig("file"));
    assertEquals(first, FileSystems.getDefaultConfig("FILE"));

    FileSystems.setDefaultConfig("FILE", second);
    assertNotEquals(first, FileSystems.getDefaultConfig("file"));
    assertNotEquals(first, FileSystems.getDefaultConfig("FILE"));
    assertEquals(second, FileSystems.getDefaultConfig("file"));
    assertEquals(second, FileSystems.getDefaultConfig("FILE"));
  }

  @Test
  public void testSetDefaultConfigNotFound() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No FileSystemRegistrar found for scheme: [gs-s3].");
    FileSystems.setDefaultConfig("gs-s3", PipelineOptionsFactory.create());
  }

  @Test
  public void testSetDefaultConfigInvalidScheme() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Scheme: [gs:] doesn't match URI syntax");
    FileSystems.setDefaultConfig("gs:", PipelineOptionsFactory.create());
  }

  @Test
  public void testGetLocalFileSystem() throws Exception {
    assertTrue(
        FileSystems.getFileSystemInternal(URI.create("~/home/")) instanceof LocalFileSystem);
    assertTrue(
        FileSystems.getFileSystemInternal(URI.create("file://home")) instanceof LocalFileSystem);
    assertTrue(
        FileSystems.getFileSystemInternal(URI.create("FILE://home")) instanceof LocalFileSystem);
    assertTrue(
        FileSystems.getFileSystemInternal(URI.create("File://home")) instanceof LocalFileSystem);
  }

  @Test
  public void testVerifySchemesAreUnique() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Scheme: [file] has conflicting registrars");
    FileSystems.verifySchemesAreUnique(
        Sets.<FileSystemRegistrar>newHashSet(
            new LocalFileSystemRegistrar(),
            new FileSystemRegistrar() {
              @Override
              public FileSystem fromOptions(@Nullable PipelineOptions options) {
                return null;
              }

              @Override
              public String getScheme() {
                return "FILE";
              }
            }));
  }
}
