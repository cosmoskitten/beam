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
package org.apache.beam.examples.complete.game.utils;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Generate, format, and write rows. Use provided information about the field names and types, as
 * well as lambda functions that describe how to generate their values.
 */
public class WriteToText<InputT>
    extends PTransform<PCollection<InputT>, PDone> {

  private static final DateTimeFormatter formatter =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
          .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));

  protected String filenamePrefix;
  protected Map<String, FieldFn<InputT>> fieldFn;
  protected boolean windowed;

  public WriteToText() {
  }

  public WriteToText(
      String filenamePrefix,
      Map<String, FieldFn<InputT>> fieldFn,
      boolean windowed) {
    this.filenamePrefix = filenamePrefix;
    this.fieldFn = fieldFn;
    this.windowed = windowed;
  }

  /**
   * A {@link Serializable} function from a {@link DoFn.ProcessContext}
   * and {@link BoundedWindow} to the value for that field.
   */
  public interface FieldFn<InputT> extends Serializable {
    Object apply(DoFn<InputT, String>.ProcessContext context, BoundedWindow window);
  }

  /** Convert each key/score pair into a row as specified by fieldFn. */
  protected class BuildRowFn extends DoFn<InputT, String> {

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      List<String> fields = new ArrayList<String>();
      for (Map.Entry<String, FieldFn<InputT>> entry : fieldFn.entrySet()) {
        String key = entry.getKey();
        FieldFn<InputT> fcn = entry.getValue();
        fields.add(key + ": " + fcn.apply(c, window));
      }
      String result = fields.stream().collect(Collectors.joining(", "));
      c.output(result);
    }
  }

  /**
   * A {@link DoFn} that writes elements to files with names deterministically derived from the
   * lower and upper bounds of their key (an {@link IntervalWindow}).
   */
  protected class WriteOneFilePerWindow extends PTransform<PCollection<String>, PDone> {

    private final String filenamePrefix;

    public WriteOneFilePerWindow(String filenamePrefix) {
      this.filenamePrefix = filenamePrefix;
    }

    @Override
    public PDone expand(PCollection<String> input) {
      // Verify that the input has a compatible window type.
      verify(input.getWindowingStrategy().getWindowFn().windowCoder() == IntervalWindow.getCoder());

      // filenamePrefix may contain a directory and a filename component. Pull out only the filename
      // component from that path for the PerWindowFiles.
      String prefix = "";
      ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(filenamePrefix);
      if (!resource.isDirectory()) {
        prefix = verifyNotNull(
            resource.getFilename(),
            "A non-directory resource should have a non-null filename: %s",
            resource);
      }

      return input.apply(
          TextIO.write()
              .to(resource.getCurrentDirectory())
              .withFilenamePolicy(new PerWindowFiles(prefix))
              .withWindowedWrites()
              .withNumShards(3));
    }
  }

  /**
   * A {@link FilenamePolicy} produces a base file name for a write based on metadata about the data
   * being written. This always includes the shard number and the total number of shards. For
   * windowed writes, it also includes the window and pane index (a sequence number assigned to each
   * trigger firing).
   */
  protected static class PerWindowFiles extends FilenamePolicy {

    private final String prefix;

    public PerWindowFiles(String prefix) {
      this.prefix = prefix;
    }

    public String filenamePrefixForWindow(IntervalWindow window) {
      return String.format("%s-%s-%s",
          prefix, formatter.print(window.start()), formatter.print(window.end()));
    }

    @Override
    public ResourceId windowedFilename(
        ResourceId outputDirectory, WindowedContext context, String extension) {
      IntervalWindow window = (IntervalWindow) context.getWindow();
      String filename = String.format(
          "%s-%s-of-%s%s",
          filenamePrefixForWindow(window), context.getShardNumber(), context.getNumShards(),
          extension);
      return outputDirectory.resolve(filename, StandardResolveOptions.RESOLVE_FILE);
    }

    @Override
    public ResourceId unwindowedFilename(
        ResourceId outputDirectory, Context context, String extension) {
      throw new UnsupportedOperationException("Unsupported.");
    }
  }

  @Override
  public PDone expand(PCollection<InputT> teamAndScore) {
    if (windowed) {
      teamAndScore
          .apply("ConvertToRow", ParDo.of(new BuildRowFn()))
          .apply(new WriteToText.WriteOneFilePerWindow(filenamePrefix));
    } else {
      teamAndScore
          .apply("ConvertToRow", ParDo.of(new BuildRowFn()))
          .apply(TextIO.write().to(filenamePrefix));
    }
    return PDone.in(teamAndScore.getPipeline());
  }
}
