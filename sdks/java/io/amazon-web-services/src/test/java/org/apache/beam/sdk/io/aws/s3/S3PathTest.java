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

package org.apache.beam.sdk.io.aws.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests {@link S3Path}.
 */
@RunWith(JUnit4.class)
public class S3PathTest {

  static final class TestCase {

    final String uri;
    final String expectedBucket;
    final String expectedKey;
    final String[] namedComponents;

    TestCase(String uri, String... namedComponents) {
      this.uri = uri;
      this.expectedBucket = namedComponents[0];
      this.namedComponents = namedComponents;
      this.expectedKey = uri.substring(expectedBucket.length() + 6);
    }
  }

  // Each test case is an expected URL, then the components used to build it.
  // Empty components result in a double slash.
  static final List<TestCase> PATH_TEST_CASES =
      Arrays.asList(
          new TestCase("s3://bucket/then/object", "bucket", "then", "object"),
          new TestCase("s3://bucket//then/object", "bucket", "", "then", "object"),
          new TestCase("s3://bucket/then//object", "bucket", "then", "", "object"),
          new TestCase("s3://bucket/then///object", "bucket", "then", "", "", "object"),
          new TestCase("s3://bucket/then/object/", "bucket", "then", "object/"),
          new TestCase("s3://bucket/then/object/", "bucket", "then/", "object/"),
          new TestCase("s3://bucket/then/object//", "bucket", "then", "object", ""),
          new TestCase("s3://bucket/then/object//", "bucket", "then", "object/", ""),
          new TestCase("s3://bucket/09azAZ!-_.*'()", "bucket", "09azAZ!-_.*'()"),
          new TestCase("s3://bucket/then--object", "bucket", "then--object"),
          new TestCase("s3://bucket-hyphenated/object", "bucket-hyphenated", "object"),
          new TestCase("s3://sub.bucket/object", "sub.bucket", "object"),
          new TestCase("s3://bucket/", "bucket"));

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testS3PathParsing() throws Exception {
    for (TestCase testCase : PATH_TEST_CASES) {
      String uriString = testCase.uri;

      S3Path path = S3Path.fromUri(URI.create(uriString));
      // Deconstruction - check bucket, key, and components.
      assertEquals(testCase.expectedBucket, path.getBucket());
      assertEquals(testCase.expectedKey, path.getKey());
      assertEquals(testCase.uri, testCase.namedComponents.length, path.getNameCount());

      // Construction - check that the path can be built from components.
      S3Path built = S3Path.fromComponents(null, null);
      for (String component : testCase.namedComponents) {
        built = built.resolve(component);
      }
      assertEquals(testCase.uri, built.toString());
    }
  }

  @Test
  public void testParentRelationship() throws Exception {
    S3Path path = S3Path.fromComponents("bucket", "then/object");
    assertEquals("bucket", path.getBucket());
    assertEquals("then/object", path.getKey());
    assertEquals(3, path.getNameCount());
    assertTrue(path.endsWith("object"));
    assertTrue(path.startsWith("bucket/then"));

    S3Path parent = path.getParent(); // s3://bucket/then/
    assertEquals("bucket", parent.getBucket());
    assertEquals("then/", parent.getKey());
    assertEquals(2, parent.getNameCount());
    assertThat(path, Matchers.not(Matchers.equalTo(parent)));
    assertTrue(path.startsWith(parent));
    assertFalse(parent.startsWith(path));
    assertTrue(parent.endsWith("then/"));
    assertTrue(parent.startsWith("bucket/then"));
    assertTrue(parent.isAbsolute());

    S3Path root = path.getRoot();
    assertEquals(0, root.getNameCount());
    assertEquals("s3://", root.toString());
    assertEquals("", root.getBucket());
    assertEquals("", root.getKey());
    assertTrue(root.isAbsolute());
    assertThat(root, Matchers.equalTo(parent.getRoot()));

    S3Path grandParent = parent.getParent(); // s3://bucket/
    assertEquals(1, grandParent.getNameCount());
    assertEquals("s3://bucket/", grandParent.toString());
    assertTrue(grandParent.isAbsolute());
    assertThat(root, Matchers.equalTo(grandParent.getParent()));
    assertThat(root.getParent(), Matchers.nullValue());

    assertTrue(path.startsWith(path.getRoot()));
    assertTrue(parent.startsWith(path.getRoot()));
  }

  @Test
  public void testRelativeParent() throws Exception {
    S3Path path = S3Path.fromComponents(null, "a/b");
    S3Path parent = path.getParent();
    assertEquals("a/", parent.toString());

    S3Path grandParent = parent.getParent();
    assertNull(grandParent);
  }

  @Test
  public void testUriSupport() throws Exception {
    URI uri = URI.create("s3://bucket/some/path");

    S3Path path = S3Path.fromUri(uri);
    assertEquals("bucket", path.getBucket());
    assertEquals("some/path", path.getKey());

    URI reconstructed = path.toUri();
    assertEquals(uri, reconstructed);

    path = S3Path.fromUri("s3://bucket");
    assertEquals("s3://bucket/", path.toString());
  }

  @Test
  public void testBucketParsing() throws Exception {
    S3Path path = S3Path.fromUri("s3://bucket");
    S3Path path2 = S3Path.fromUri("s3://bucket/");

    assertEquals(path, path2);
    assertEquals(path.toString(), path2.toString());
    assertEquals(path.toUri(), path2.toUri());
  }

  @Test
  public void testS3PathToString() throws Exception {
    String filename = "s3://some-bucket/some/file.txt";
    S3Path path = S3Path.fromUri(filename);
    assertEquals(filename, path.toString());
  }

  @Test
  public void testEquals() {
    S3Path a = S3Path.fromComponents(null, "a/b/c");
    S3Path a2 = S3Path.fromComponents(null, "a/b/c");
    assertFalse(a.isAbsolute());
    assertFalse(a2.isAbsolute());

    S3Path b = S3Path.fromComponents("bucket", "a/b/c");
    S3Path b2 = S3Path.fromComponents("bucket", "a/b/c");
    assertTrue(b.isAbsolute());
    assertTrue(b2.isAbsolute());

    assertEquals(a, a);
    assertThat(a, Matchers.not(Matchers.equalTo(b)));
    assertThat(b, Matchers.not(Matchers.equalTo(a)));

    assertEquals(a, a2);
    assertEquals(a2, a);
    assertEquals(b, b2);
    assertEquals(b2, b);

    assertThat(a, Matchers.not(Matchers.equalTo(Paths.get("/tmp/foo"))));
    assertTrue(a != null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidS3Path() {
    @SuppressWarnings("unused")
    S3Path filename = S3Path.fromUri("file://invalid/s3/path");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidBucket() {
    S3Path.fromComponents("invalid/", "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidBucketWithUnderscore() {
    S3Path.fromComponents("invalid_bucket", "");
  }

  @Test
  public void testResolveUri() {
    S3Path path = S3Path.fromComponents("bucket", "a/b/c");
    S3Path d = path.resolve("s3://bucket2/d");
    assertEquals("s3://bucket2/d", d.toString());
  }

  @Test
  public void testResolveOther() {
    S3Path a = S3Path.fromComponents("bucket", "a");
    S3Path b = a.resolve(Paths.get("b"));
    assertEquals("a/b", b.getKey());
  }

  @Test
  public void testGetFileName() {
    assertEquals("foo", S3Path.fromUri("s3://bucket/bar/foo").getFileName().toString());
    assertEquals("foo", S3Path.fromUri("s3://bucket/foo").getFileName().toString());
    thrown.expect(UnsupportedOperationException.class);
    S3Path.fromUri("s3://bucket/").getFileName();
  }

  @Test
  public void testResolveSibling() {
    assertEquals(
        "s3://bucket/bar/moo",
        S3Path.fromUri("s3://bucket/bar/foo").resolveSibling("moo").toString());
    assertEquals(
        "s3://bucket/moo", S3Path.fromUri("s3://bucket/foo").resolveSibling("moo").toString());
    thrown.expect(UnsupportedOperationException.class);
    S3Path.fromUri("s3://bucket/").resolveSibling("moo");
  }

  @Test
  public void testCompareTo() {
    S3Path a = S3Path.fromComponents("bucket", "a");
    S3Path b = S3Path.fromComponents("bucket", "b");
    S3Path b2 = S3Path.fromComponents("bucket2", "b");
    S3Path brel = S3Path.fromComponents(null, "b");
    S3Path a2 = S3Path.fromComponents("bucket", "a");
    S3Path arel = S3Path.fromComponents(null, "a");

    assertThat(a.compareTo(b), Matchers.lessThan(0));
    assertThat(b.compareTo(a), Matchers.greaterThan(0));
    assertThat(a.compareTo(a2), Matchers.equalTo(0));

    assertThat(a.hashCode(), Matchers.equalTo(a2.hashCode()));
    assertThat(a.hashCode(), Matchers.not(Matchers.equalTo(b.hashCode())));
    assertThat(b.hashCode(), Matchers.not(Matchers.equalTo(brel.hashCode())));

    assertThat(brel.compareTo(b), Matchers.lessThan(0));
    assertThat(b.compareTo(brel), Matchers.greaterThan(0));
    assertThat(arel.compareTo(brel), Matchers.lessThan(0));
    assertThat(brel.compareTo(arel), Matchers.greaterThan(0));

    assertThat(b.compareTo(b2), Matchers.lessThan(0));
    assertThat(b2.compareTo(b), Matchers.greaterThan(0));
  }

  @Test
  public void testCompareTo_ordering() {
    S3Path ab = S3Path.fromComponents("bucket", "a/b");
    S3Path abc = S3Path.fromComponents("bucket", "a/b/c");
    S3Path a1b = S3Path.fromComponents("bucket", "a-1/b");

    assertThat(ab.compareTo(a1b), Matchers.lessThan(0));
    assertThat(a1b.compareTo(ab), Matchers.greaterThan(0));

    assertThat(ab.compareTo(abc), Matchers.lessThan(0));
    assertThat(abc.compareTo(ab), Matchers.greaterThan(0));
  }

  @Test
  public void testCompareTo_buckets() {
    S3Path a = S3Path.fromComponents(null, "a/b/c");
    S3Path b = S3Path.fromComponents("bucket", "a/b/c");

    assertThat(a.compareTo(b), Matchers.lessThan(0));
    assertThat(b.compareTo(a), Matchers.greaterThan(0));
  }

  @Test
  public void testIterator() {
    S3Path a = S3Path.fromComponents("bucket", "a/b/c");
    Iterator<Path> it = a.iterator();

    assertTrue(it.hasNext());
    assertEquals("s3://bucket/", it.next().toString());
    assertTrue(it.hasNext());
    assertEquals("a", it.next().toString());
    assertTrue(it.hasNext());
    assertEquals("b", it.next().toString());
    assertTrue(it.hasNext());
    assertEquals("c", it.next().toString());
    assertFalse(it.hasNext());
  }

  @Test
  public void testSubpath() {
    S3Path a = S3Path.fromComponents("bucket", "a/b/c/d");
    assertThat(a.subpath(0, 1).toString(), Matchers.equalTo("s3://bucket/"));
    assertThat(a.subpath(0, 2).toString(), Matchers.equalTo("s3://bucket/a"));
    assertThat(a.subpath(0, 3).toString(), Matchers.equalTo("s3://bucket/a/b"));
    assertThat(a.subpath(0, 4).toString(), Matchers.equalTo("s3://bucket/a/b/c"));
    assertThat(a.subpath(1, 2).toString(), Matchers.equalTo("a"));
    assertThat(a.subpath(2, 3).toString(), Matchers.equalTo("b"));
    assertThat(a.subpath(2, 4).toString(), Matchers.equalTo("b/c"));
    assertThat(a.subpath(2, 5).toString(), Matchers.equalTo("b/c/d"));
  }

  @Test
  public void testGetName() {
    S3Path a = S3Path.fromComponents("bucket", "a/b/c/d");
    assertEquals(5, a.getNameCount());
    assertThat(a.getName(0).toString(), Matchers.equalTo("s3://bucket/"));
    assertThat(a.getName(1).toString(), Matchers.equalTo("a"));
    assertThat(a.getName(2).toString(), Matchers.equalTo("b"));
    assertThat(a.getName(3).toString(), Matchers.equalTo("c"));
    assertThat(a.getName(4).toString(), Matchers.equalTo("d"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSubPathError() {
    S3Path a = S3Path.fromComponents("bucket", "a/b/c/d");
    a.subpath(1, 1); // throws IllegalArgumentException
    fail();
  }
}
