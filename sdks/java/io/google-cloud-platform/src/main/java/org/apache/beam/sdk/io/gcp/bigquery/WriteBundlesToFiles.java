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

package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.resolveTempLocation;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.WriteBundlesToFiles.Result;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes each bundle of {@link TableRow} elements out to separate file using {@link
 * TableRowWriter}. Elements destined to different destinations are written to separate files.
 * The transform will not write an element to a file if it is already writing to
 * {@link #maxNumWritersPerBundle} files and the element is destined to a new destination. In this
 * case, the element will be spilled into the output, and the {@link WriteGroupedRecordsToFiles}
 * transform will take care of writing it to a file.
 */
class WriteBundlesToFiles<DestinationT>
    extends DoFn<KV<DestinationT, TableRow>, Result<DestinationT>> {
  private static final Logger LOG = LoggerFactory.getLogger(WriteBundlesToFiles.class);

  // When we spill records, shard the output keys to prevent hotspots. Experiments running up to
  // 10TB of data have shown a sharding of 10 to be a good choice.
  private static final int SPILLED_RECORD_SHARDING_FACTOR = 10;

  // Map from tablespec to a writer for that table.
  private transient Map<DestinationT, TableRowWriter> writers;
  private transient Map<DestinationT, BoundedWindow> writerWindows;
  private final String stepUuid;
  private final TupleTag<KV<ShardedKey<DestinationT>, TableRow>> unwrittedRecordsTag;
  private int maxNumWritersPerBundle;
  /**
   * The result of the {@link WriteBundlesToFiles} transform. Corresponds to a single output file,
   * and encapsulates the table it is destined to as well as the file byte size.
   */
  public static final class Result<DestinationT> implements Serializable {
    private static final long serialVersionUID = 1L;
    public final String filename;
    public final Long fileByteSize;
    public final DestinationT destination;

    public Result(String filename, Long fileByteSize, DestinationT destination) {
      this.filename = filename;
      this.fileByteSize = fileByteSize;
      this.destination = destination;
    }
  }

  /** a coder for the {@link Result} class. */
  public static class ResultCoder<DestinationT> extends StructuredCoder<Result<DestinationT>> {
    private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
    private static final VarLongCoder longCoder = VarLongCoder.of();
    private final Coder<DestinationT> destinationCoder;

    public static <DestinationT> ResultCoder<DestinationT> of(
        Coder<DestinationT> destinationCoder) {
      return new ResultCoder<>(destinationCoder);
    }

    ResultCoder(Coder<DestinationT> destinationCoder) {
      this.destinationCoder = destinationCoder;
    }

    @Override
    public void encode(Result<DestinationT> value, OutputStream outStream, Context context)
        throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null value");
      }
      stringCoder.encode(value.filename, outStream, context.nested());
      longCoder.encode(value.fileByteSize, outStream, context.nested());
      destinationCoder.encode(value.destination, outStream, context.nested());
    }

    @Override
    public Result<DestinationT> decode(InputStream inStream, Context context) throws IOException {
      String filename = stringCoder.decode(inStream, context.nested());
      long fileByteSize = longCoder.decode(inStream, context.nested());
      DestinationT destination = destinationCoder.decode(inStream, context.nested());
      return new Result<>(filename, fileByteSize, destination);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.singletonList(destinationCoder);
    }

    @Override
    public void verifyDeterministic() {}
  }

  WriteBundlesToFiles(
      String stepUuid,
      TupleTag<KV<ShardedKey<DestinationT>, TableRow>> unwrittedRecordsTag,
      int maxNumWritersPerBundle) {
    this.stepUuid = stepUuid;
    this.unwrittedRecordsTag = unwrittedRecordsTag;
    this.maxNumWritersPerBundle = maxNumWritersPerBundle;
  }

  @StartBundle
  public void startBundle() {
    // This must be done for each bundle, as by default the {@link DoFn} might be reused between
    // bundles.
    this.writers = Maps.newHashMap();
    this.writerWindows = Maps.newHashMap();
  }

  TableRowWriter createAndInsertWriter(DestinationT destination, String tempFilePrefix,
                                       BoundedWindow window) throws Exception {
    TableRowWriter writer = new TableRowWriter(tempFilePrefix);
    writer.open(UUID.randomUUID().toString());
    writers.put(destination, writer);
    writerWindows.put(destination, window);
    LOG.debug("Done opening writer {}", writer);
    return writer;
  }

  @ProcessElement
  public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
    String tempFilePrefix = resolveTempLocation(
        c.getPipelineOptions().getTempLocation(), "BigQueryWriteTemp", stepUuid);
    TableRowWriter writer = writers.get(c.element().getKey());
    if (writer != null && writer.getByteSize() > Write.MAX_FILE_SIZE) {
      // File is too big. Close it and open a new file.
      TableRowWriter.Result result = writer.close();
      c.output(new Result<>(result.resourceId.toString(), result.byteSize, c.element().getKey()));
      writer = createAndInsertWriter(c.element().getKey(), tempFilePrefix, window);
    }

    if (writer == null) {
      // Only create a new writer if we have fewer than maxNumWritersPerBundle already in this
      // bundle.
      if (writers.size() <= maxNumWritersPerBundle) {
        writer = createAndInsertWriter(c.element().getKey(), tempFilePrefix, window);
      } else {
        // This means that we already had too many writers open in this bundle. "spill" this record
        // into the output. It will be grouped and written to a file in a subsequent stage.
        c.output(unwrittedRecordsTag,
            KV.of(ShardedKey.of(c.element().getKey(),
                ThreadLocalRandom.current().nextInt(SPILLED_RECORD_SHARDING_FACTOR)),
                c.element().getValue()));
      }
    }
    if (writer != null) {
      try {
        writer.write(c.element().getValue());
      } catch (Exception e) {
        // Discard write result and close the write.
        try {
          writer.close();
          // The writer does not need to be reset, as this DoFn cannot be reused.
        } catch (Exception closeException) {
          // Do not mask the exception that caused the write to fail.
          e.addSuppressed(closeException);
        }
        throw e;
      }
    }
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext c) throws Exception {
    List<Exception> exceptionList = Lists.newArrayList();
    for (Map.Entry<DestinationT, TableRowWriter> entry : writers.entrySet()) {
      try {
        TableRowWriter.Result result = entry.getValue().close();
        c.output(new Result<>(result.resourceId.toString(), result.byteSize, entry.getKey()),
            writerWindows.get(entry.getKey()).maxTimestamp(),
            writerWindows.get(entry.getKey()));
      } catch (Exception e) {
        exceptionList.add(e);
      }
    }
    writers.clear();

    if (!exceptionList.isEmpty()) {
      Exception e = new RuntimeException("Exceptions thrown while finishing bundle");
      for (Exception thrown : exceptionList) {
        e.addSuppressed(thrown);
      }
      throw e;
    }
  }
}
