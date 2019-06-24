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

import com.google.auto.value.AutoValue;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

/** This returns a row count estimation for files associated with a file pattern. */
@AutoValue
public abstract class TextRowCountEstimator {
  private static final long DEFAULT_NUM_BYTES_LINES = 1024L;
  private static final Compression DEFAULT_COMPRESSION = Compression.AUTO;
  private static final FileIO.ReadMatches.DirectoryTreatment DEFAULT_DIRECTORY_TREATMENT =
      FileIO.ReadMatches.DirectoryTreatment.SKIP;
  private static final EmptyMatchTreatment DEFAULT_EMPTY_MATCH_TREATMENT =
      EmptyMatchTreatment.DISALLOW;

  public abstract int getNumSampledFiles();

  public boolean isSamplingAllFiles() {
    return getNumSampledFiles() == -1;
  }

  public abstract long getNumSampledBytes();

  @Nullable
  @SuppressWarnings("mutable")
  public abstract byte[] getDelimiters();

  public abstract String getFilePattern();

  public abstract Compression getCompression();

  public abstract EmptyMatchTreatment getEmptyMatchTreatment();

  public abstract FileIO.ReadMatches.DirectoryTreatment getDirectoryTreatment();

  public static TextRowCountEstimator.Builder builder() {
    return (new AutoValue_TextRowCountEstimator.Builder())
        .sampleAllFiles()
        .setNumSampledBytes(DEFAULT_NUM_BYTES_LINES)
        .setCompression(DEFAULT_COMPRESSION)
        .setDirectoryTreatment(DEFAULT_DIRECTORY_TREATMENT)
        .setEmptyMatchTreatment(DEFAULT_EMPTY_MATCH_TREATMENT);
  }

  /** Builder for {@link org.apache.beam.sdk.io.TextRowCountEstimator}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setNumSampledFiles(int numSampledFiles);

    public abstract Builder setNumSampledBytes(long numSampledBytes);

    public abstract Builder setDirectoryTreatment(
        FileIO.ReadMatches.DirectoryTreatment directoryTreatment);

    public abstract Builder setCompression(Compression compression);

    public abstract Builder setDelimiters(byte[] delimiters);

    public abstract Builder setFilePattern(String filePattern);

    public abstract Builder setEmptyMatchTreatment(EmptyMatchTreatment emptyMatchTreatment);

    public Builder sampleAllFiles() {
      return this.setNumSampledFiles(-1);
    }

    public abstract TextRowCountEstimator build();
  }

  /**
   * Estimates the number of non empty rows. Note that it does not cache the previous estimate. It
   * returns 0L if all the lines that have been read are empty.
   *
   * @return Number of estimated rows.
   * @throws IOException
   */
  public Long estimateRowCount(PipelineOptions pipelineOptions) throws IOException {
    long linesSize = 0;
    int numberOfReadLines = 0;
    long totalFileSizes = 0;
    int numberOfReadFiles = 0;

    MatchResult match = FileSystems.match(getFilePattern(), getEmptyMatchTreatment());

    for (MatchResult.Metadata metadata : match.metadata()) {

      if ((!isSamplingAllFiles()) && numberOfReadFiles > getNumSampledFiles()) {
        break;
      }

      FileIO.ReadableFile file =
          FileIO.ReadMatches.matchToReadableFile(
              metadata, getDirectoryTreatment(), getCompression());

      long readingWindowSize = Math.min(getNumSampledBytes(), metadata.sizeBytes());
      OffsetRange range = new OffsetRange(0, readingWindowSize);

      TextSource textSource =
          new TextSource(
              ValueProvider.StaticValueProvider.of(file.getMetadata().resourceId().toString()),
              getEmptyMatchTreatment(),
              getDelimiters());
      FileBasedSource<String> source =
          CompressedSource.from(textSource).withCompression(file.getCompression());
      try (BoundedSource.BoundedReader<String> reader =
          source
              .createForSubrangeOfFile(file.getMetadata(), range.getFrom(), range.getTo())
              .createReader(pipelineOptions)) {
        for (boolean more = reader.start(); more; more = reader.advance()) {
          int length = reader.getCurrent().length();
          linesSize += reader.getCurrent().length() + 1; // for \n at the end of line
          numberOfReadLines += length == 0 ? 0 : 1;
        }
      }
      long fileSize = metadata.sizeBytes();
      numberOfReadFiles += fileSize == 0 ? 0 : 1;
      totalFileSizes += fileSize;
    }

    if (totalFileSizes == 0) {
      return 0L;
    }

    if (numberOfReadLines == 0) {
      return 0L;
    }

    return totalFileSizes * numberOfReadLines / linesSize;
  }
}
