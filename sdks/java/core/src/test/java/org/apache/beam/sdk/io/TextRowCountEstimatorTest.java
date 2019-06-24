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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v20_0.com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link org.apache.beam.sdk.io.TextRowCountEstimator}. */
@RunWith(JUnit4.class)
public class TextRowCountEstimatorTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private static final Logger LOG = LoggerFactory.getLogger(TextRowCountEstimatorTest.class);

  @Test
  public void testNonEmptyFiles() throws IOException {
    File file1 = temporaryFolder.newFile("file1.txt");
    Writer writer = Files.newWriter(file1, Charsets.UTF_8);
    for (int i = 0; i < 100; i++) {
      writer.write("123123123\n");
    }
    writer.flush();
    writer.close();
    temporaryFolder.newFolder("testfolder");
    temporaryFolder.newFolder("testfolder2");
    file1 = temporaryFolder.newFile("testfolder/test2.txt");
    writer = Files.newWriter(file1, Charsets.UTF_8);
    for (int i = 0; i < 50; i++) {
      writer.write("123123123\n");
    }

    writer.flush();
    writer.close();
    TextRowCountEstimator textRowCountEstimator =
        TextRowCountEstimator.builder().setFilePattern(temporaryFolder.getRoot() + "/**").build();
    Long rows = textRowCountEstimator.estimateRowCount(PipelineOptionsFactory.create());
    Assert.assertNotNull(rows);
    Assert.assertEquals(150L, rows.longValue());
  }

  @Test(expected = FileNotFoundException.class)
  public void testEmptyFolder() throws IOException {
    TextRowCountEstimator textRowCountEstimator =
        TextRowCountEstimator.builder().setFilePattern(temporaryFolder.getRoot() + "/**").build();
    Long rows = textRowCountEstimator.estimateRowCount(PipelineOptionsFactory.create());
  }

  @Test
  public void testEmptyFile() throws IOException {
    File file1 = temporaryFolder.newFile("file1.txt");
    Writer writer = Files.newWriter(file1, Charsets.UTF_8);
    for (int i = 0; i < 100; i++) {
      writer.write("\n");
    }
    writer.flush();
    writer.close();
    TextRowCountEstimator textRowCountEstimator =
        TextRowCountEstimator.builder().setFilePattern(temporaryFolder.getRoot() + "/**").build();
    Long rows = textRowCountEstimator.estimateRowCount(PipelineOptionsFactory.create());
    Assert.assertEquals(0L, rows.longValue());
  }

  @Test(expected = FileNotFoundException.class)
  public void testNonExistance() throws IOException {
    TextRowCountEstimator textRowCountEstimator =
        TextRowCountEstimator.builder()
            .setFilePattern(temporaryFolder.getRoot() + "/something/**")
            .build();
    Long rows = textRowCountEstimator.estimateRowCount(PipelineOptionsFactory.create());
    Assert.assertNull(rows);
  }
}
