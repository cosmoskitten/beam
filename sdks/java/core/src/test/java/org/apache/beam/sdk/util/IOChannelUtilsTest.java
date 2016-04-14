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
import static org.junit.Assert.fail;

import org.apache.beam.sdk.io.FileNameTemplate;
import com.google.common.io.Files;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * Tests for IOChannelUtils.
 */
@RunWith(JUnit4.class)
public class IOChannelUtilsTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test(expected = IllegalArgumentException.class)
  public void testShardNameCollision() throws Exception {
    File outFolder = tmpFolder.newFolder();
    String filename = outFolder.toPath().resolve("output").toString();

    IOChannelUtils.create(FileNameTemplate.of(filename, "", ""), 2, "text").close();
    fail("IOChannelUtils.create expected to fail due "
        + "to filename collision");
  }

  @Test
  public void testGetSizeBytes() throws Exception {
    String data = "TestData";
    File file = tmpFolder.newFile();
    Files.write(data, file, StandardCharsets.UTF_8);
    assertEquals(data.length(), IOChannelUtils.getSizeBytes(file.getPath()));
  }

  @Test
  public void testResolve() throws Exception {
    String expected = tmpFolder.getRoot().toPath().resolve("aa").toString();
    assertEquals(expected, IOChannelUtils.resolve(tmpFolder.getRoot().toString(), "aa"));
  }
}
