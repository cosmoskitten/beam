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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.io.DecoratedFileSink.DecoratedFileWriter;
import org.apache.beam.sdk.io.DecoratedFileSink.WriterOutputDecorator;
import org.apache.beam.sdk.io.DecoratedFileSink.WriterOutputDecoratorFactory;
import org.apache.beam.sdk.io.FileBasedSink.FileResult;
import org.apache.beam.sdk.util.MimeTypes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests {@link DecoratedFileSink} abstract functionality using simple
 * {@link WriterOutputDecoratorFactory} implementation.
 */
@RunWith(JUnit4.class)
public class DecoratedFileSinkTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final String TEMPORARY_FILENAME_SEPARATOR = "-temp-";
  private String baseOutputFilename = "output";
  private String baseTemporaryFilename = "temp";

  private String appendToTempFolder(String filename) {
    return Paths.get(tmpFolder.getRoot().getPath(), filename).toString();
  }

  private String getBaseOutputFilename() {
    return appendToTempFolder(baseOutputFilename);
  }

  /**
   * Assert that a file contains the lines provided, in the same order as expected.
   */
  private void assertFileContains(List<String> expected, String filename) throws Exception {
    try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
      List<String> actual = new ArrayList<>();
      for (;;) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        actual.add(line);
      }
      assertEquals(expected, actual);
    }
  }

  /**
   * {@link DecoratedFileWriter} writes to the {@link WriterOutputDecorator} provided by
   * {@link WriterOutputDecoratorFactory}.
   */
  @Test
  public void testDecoratedFileWriter() throws Exception {
    final String testUid = "testId";
    final String expectedFilename =
        getBaseOutputFilename() + TEMPORARY_FILENAME_SEPARATOR + testUid;
    final DecoratedFileWriter<String> writer =
        new DecoratedFileSink<String>(getBaseOutputFilename(), "txt", TextIO.DEFAULT_TEXT_CODER,
            "h", "f", new SimpleDecoratorFactory()).createWriteOperation(null).createWriter(null);

    final List<String> expected = new ArrayList<>();
    expected.add("hh");
    expected.add("");
    expected.add("aa");
    expected.add("");
    expected.add("bb");
    expected.add("");
    expected.add("ff");
    expected.add("");

    writer.open(testUid);
    writer.write("a");
    writer.write("b");
    final FileResult result = writer.close();

    assertEquals(expectedFilename, result.getFilename());
    assertFileContains(expected, expectedFilename);
  }

  private static class SimpleDecoratorFactory implements WriterOutputDecoratorFactory {
    @Override
    public WriterOutputDecorator create(OutputStream out) throws IOException {
      return new SimpleDecorator(out);
    }

    @Override
    public String getMimeType() {
      return MimeTypes.TEXT;
    }

    private static class SimpleDecorator extends WriterOutputDecorator {
      public SimpleDecorator(final OutputStream out) {
        // OutputStream just writes each byte twice.
        super(new OutputStream() {
          @Override
          public void write(int b) throws IOException {
            out.write(b);
            out.write(b);
          }
        });
      }
    }
  }
}
