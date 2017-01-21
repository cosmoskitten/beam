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

import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
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

  @Before
  public void setup() {
    FileSystems.setDefaultConfigInWorkers(PipelineOptionsFactory.create());
  }

  @Test
  public void testGetLocalFileSystem() throws Exception {
    assertTrue(
        FileSystems.getFileSystemInternal("~/home/") instanceof LocalFileSystem);
    assertTrue(
        FileSystems.getFileSystemInternal("file://home") instanceof LocalFileSystem);
    assertTrue(
        FileSystems.getFileSystemInternal("FILE://home") instanceof LocalFileSystem);
    assertTrue(
        FileSystems.getFileSystemInternal("File://home") instanceof LocalFileSystem);
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

  @Test
  public void testShardFormatExpansion() {
    assertEquals("output-001-of-123.txt",
        FileSystems.constructName("output", "-SSS-of-NNN",
            ".txt",
            1, 123));

    assertEquals("out.txt/part-00042",
        FileSystems.constructName("out.txt", "/part-SSSSS", "",
            42, 100));

    assertEquals("out.txt",
        FileSystems.constructName("ou", "t.t", "xt", 1, 1));

    assertEquals("out0102shard.txt",
        FileSystems.constructName("out", "SSNNshard", ".txt", 1, 2));

    assertEquals("out-2/1.part-1-of-2.txt",
        FileSystems.constructName("out", "-N/S.part-S-of-N",
            ".txt", 1, 2));
  }
}
