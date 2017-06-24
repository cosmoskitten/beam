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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.CompressedSource.CompressionMode;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.WritableByteChannelFactory;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRange;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * {@link PTransform}s for reading and writing text files.
 *
 * <p>To read a {@link PCollection} from one or more text files, use {@code TextIO.read()} to
 * instantiate a transform and use {@link TextIO.Read#from(String)} to specify the path of the
 * file(s) to be read. Alternatively, if the filenames to be read are themselves in a
 * {@link PCollection}, apply {@link TextIO#readAll()}.
 *
 * <p>{@link TextIO.Read} returns a {@link PCollection} of {@link String Strings}, each
 * corresponding to one line of an input UTF-8 text file (split into lines delimited by '\n', '\r',
 * or '\r\n').
 *
 * <p>Example 1: reading a file or filepattern.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local file (only runs locally):
 * PCollection<String> lines = p.apply(TextIO.read().from("/local/path/to/file.txt"));
 * }</pre>
 *
 * <p>Example 2: reading a PCollection of filenames.
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // E.g. the filenames might be computed from other data in the pipeline, or
 * // read from a data source.
 * PCollection<String> filenames = ...;
 *
 * // Read all files in the collection.
 * PCollection<String> lines = filenames.apply(TextIO.readAll());
 * }</pre>
 *
 * <p>To write a {@link PCollection} to one or more text files, use {@code TextIO.write()}, using
 * {@link TextIO.Write#to(String)} to specify the output prefix of the files to write.
 *
 * <p>By default, all input is put into the global window before writing. If per-window writes are
 * desired - for example, when using a streaming runner -
 * {@link TextIO.Write#withWindowedWrites()} will cause windowing and triggering to be
 * preserved. When producing windowed writes, the number of output shards must be set explicitly
 * using {@link TextIO.Write#withNumShards(int)}; some runners may set this for you to a
 * runner-chosen value, so you may need not set it yourself. A {@link FilenamePolicy} can also be
 * set in case you need better control over naming files created by unique windows.
 * {@link DefaultFilenamePolicy} policy for producing unique filenames might not be appropriate
 * for your use case.
 *
 * <p>Any existing files with the same names as generated output files will be overwritten.
 *
 * <p>For example:
 * <pre>{@code
 * // A simple Write to a local file (only runs locally):
 * PCollection<String> lines = ...;
 * lines.apply(TextIO.write().to("/path/to/file.txt"));
 *
 * // Same as above, only with Gzip compression:
 * PCollection<String> lines = ...;
 * lines.apply(TextIO.write().to("/path/to/file.txt"));
 *      .withSuffix(".txt")
 *      .withWritableByteChannelFactory(FileBasedSink.CompressionType.GZIP));
 * }</pre>
 */
public class TextIO {
  /**
   * A {@link PTransform} that reads from one or more text files and returns a bounded
   * {@link PCollection} containing one element for each line of the input files.
   */
  public static Read read() {
    return new AutoValue_TextIO_Read.Builder().setCompressionType(CompressionType.AUTO).build();
  }

  /**
   * A {@link PTransform} that works like {@link #read}, but reads each file in a {@link
   * PCollection} of filenames.
   */
  public static ReadAll readAll() {
    return new AutoValue_TextIO_ReadAll.Builder()
        .setCompressionType(CompressionType.AUTO)
        // 64MB is a reasonable value that allows to amortize the cost of opening files,
        // but is not so large as to exhaust a typical runner's maximum amount of output per
        // ProcessElement call.
        .setDesiredBundleSizeBytes(64 * 1024 * 1024L)
        .build();
  }

  /**
   * A {@link PTransform} that writes a {@link PCollection} to a text file (or multiple text files
   * matching a sharding pattern), with each element of the input collection encoded into its own
   * line.
   */
  public static Write write() {
    return new AutoValue_TextIO_Write.Builder()
        .setFilenamePrefix(null)
        .setShardTemplate(null)
        .setFilenameSuffix(null)
        .setFilenamePolicy(null)
        .setWritableByteChannelFactory(FileBasedSink.CompressionType.UNCOMPRESSED)
        .setWindowedWrites(false)
        .setNumShards(0)
        .build();
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {
    @Nullable abstract ValueProvider<String> getFilepattern();
    abstract CompressionType getCompressionType();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);
      abstract Builder setCompressionType(CompressionType compressionType);

      abstract Read build();
    }

    /**
     * Reads text files that reads from the file(s) with the given filename or filename pattern.
     *
     * <p>This can be a local path (if running locally), or a Google Cloud Storage filename or
     * filename pattern of the form {@code "gs://<bucket>/<filepath>"} (if running locally or using
     * remote execution service).
     *
     * <p>Standard <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html" >Java
     * Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
     */
    public Read from(String filepattern) {
      checkNotNull(filepattern, "Filepattern cannot be empty.");
      return from(StaticValueProvider.of(filepattern));
    }

    /** Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}. */
    public Read from(ValueProvider<String> filepattern) {
      checkNotNull(filepattern, "Filepattern cannot be empty.");
      return toBuilder().setFilepattern(filepattern).build();
    }

    /**
     * Returns a new transform for reading from text files that's like this one but
     * reads from input sources using the specified compression type.
     *
     * <p>If no compression type is specified, the default is {@link TextIO.CompressionType#AUTO}.
     */
    public Read withCompressionType(TextIO.CompressionType compressionType) {
      return toBuilder().setCompressionType(compressionType).build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      if (getFilepattern() == null) {
        throw new IllegalStateException("need to set the filepattern of a TextIO.Read transform");
      }

      final Bounded<String> read = org.apache.beam.sdk.io.Read.from(getSource());
      PCollection<String> pcol = input.getPipeline().apply("Read", read);
      // Honor the default output coder that would have been used by this PTransform.
      pcol.setCoder(getDefaultOutputCoder());
      return pcol;
    }

    // Helper to create a source specific to the requested compression type.
    protected FileBasedSource<String> getSource() {
      return wrapWithCompression(new TextSource(getFilepattern()), getCompressionType());
    }

    private static FileBasedSource<String> wrapWithCompression(
        FileBasedSource<String> source, CompressionType compressionType) {
      switch (compressionType) {
        case UNCOMPRESSED:
          return source;
        case AUTO:
          return CompressedSource.from(source);
        case BZIP2:
          return
              CompressedSource.from(source)
                  .withDecompression(CompressionMode.BZIP2);
        case GZIP:
          return
              CompressedSource.from(source)
                  .withDecompression(CompressionMode.GZIP);
        case ZIP:
          return
              CompressedSource.from(source)
                  .withDecompression(CompressionMode.ZIP);
        case DEFLATE:
          return
              CompressedSource.from(source)
                  .withDecompression(CompressionMode.DEFLATE);
        default:
          throw new IllegalArgumentException("Unknown compression type: " + compressionType);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      String filepatternDisplay = getFilepattern().isAccessible()
        ? getFilepattern().get() : getFilepattern().toString();
      builder
          .add(DisplayData.item("compressionType", getCompressionType().toString())
            .withLabel("Compression Type"))
          .addIfNotNull(DisplayData.item("filePattern", filepatternDisplay)
            .withLabel("File Pattern"));
    }

    @Override
    protected Coder<String> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }
  }

  /** Implementation of {@link #readAll}. */
  @AutoValue
  public abstract static class ReadAll
      extends PTransform<PCollection<String>, PCollection<String>> {
    abstract CompressionType getCompressionType();
    abstract long getDesiredBundleSizeBytes();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setCompressionType(CompressionType compressionType);
      abstract Builder setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

      abstract ReadAll build();
    }

    /** Same as {@link Read#withCompressionType(CompressionType)}. */
    public ReadAll withCompressionType(CompressionType compressionType) {
      return toBuilder().setCompressionType(compressionType).build();
    }

    @VisibleForTesting
    ReadAll withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
      return input
          .apply("Expand glob", ParDo.of(new ExpandGlobFn()))
          .apply(
              "Split into ranges",
              ParDo.of(new SplitIntoRangesFn(getCompressionType(), getDesiredBundleSizeBytes())))
          // Fusion break
          .apply("Fusion break", new FusionBreak<KV<Metadata, OffsetRange>>())
          .apply("Read", ParDo.of(new ReadTextFn(this)));
    }

    private static class FusionBreak<T> extends PTransform<PCollection<T>, PCollection<T>> {
      @Override
      public PCollection<T> expand(PCollection<T> input) {
        return input
            .apply("Unique key", ParDo.of(new AssignUniqueKeyFn<T>()))
            .apply("Group", GroupByKey.<Integer, T>create())
            .apply("Values", Values.<Iterable<T>>create())
            .apply("Flatten", Flatten.<T>iterables());
      }
    }

    private static class AssignUniqueKeyFn<T> extends DoFn<T, KV<Integer, T>> {
      private int index = ThreadLocalRandom.current().nextInt();

      @ProcessElement
      public void process(ProcessContext c) {
        c.output(KV.of(++index, c.element()));
      }
    }

    private static class ExpandGlobFn extends DoFn<String, Metadata> {
      @ProcessElement
      public void process(ProcessContext c) throws Exception {
        MatchResult match = FileSystems.match(c.element());
        checkArgument(
            match.status().equals(Status.OK),
            "Failed to match filepattern %s: %s",
            c.element(),
            match.status());
        for (Metadata metadata : match.metadata()) {
          c.output(metadata);
        }
      }
    }

    private static class SplitIntoRangesFn extends DoFn<Metadata, KV<Metadata, OffsetRange>> {
      private final CompressionType compressionType;
      private final long desiredBundleSize;

      private SplitIntoRangesFn(CompressionType compressionType, long desiredBundleSize) {
        this.compressionType = compressionType;
        this.desiredBundleSize = desiredBundleSize;
      }

      @ProcessElement
      public void process(ProcessContext c) {
        Metadata metadata = c.element();
        final boolean isSplittable = isSplittable(metadata, compressionType);
        if (!isSplittable) {
          c.output(KV.of(metadata, new OffsetRange(0, metadata.sizeBytes())));
          return;
        }
        for (OffsetRange range :
            new OffsetRange(0, metadata.sizeBytes()).split(desiredBundleSize, 0)) {
          c.output(KV.of(metadata, range));
        }
      }

      static boolean isSplittable(Metadata metadata, CompressionType compressionType) {
        if (!metadata.isReadSeekEfficient()) {
          return false;
        }
        switch (compressionType) {
          case AUTO:
            return !CompressionMode.isCompressed(metadata.resourceId().toString());
          case UNCOMPRESSED:
            return true;
          case GZIP:
          case BZIP2:
          case ZIP:
          case DEFLATE:
            return false;
          default:
            throw new UnsupportedOperationException("Unknown compression type: " + compressionType);
        }
      }
    }

    private static class ReadTextFn extends DoFn<KV<Metadata, OffsetRange>, String> {
      private final TextIO.ReadAll spec;

      private ReadTextFn(ReadAll spec) {
        this.spec = spec;
      }

      @ProcessElement
      public void process(ProcessContext c) throws IOException {
        Metadata metadata = c.element().getKey();
        OffsetRange range = c.element().getValue();
        FileBasedSource<String> source =
            TextIO.Read.wrapWithCompression(
                new TextSource(StaticValueProvider.of(metadata.toString())),
                spec.getCompressionType());
        BoundedSource.BoundedReader<String> reader =
            source
                .createForSubrangeOfFile(metadata, range.getFrom(), range.getTo())
                .createReader(c.getPipelineOptions());
        for (boolean more = reader.start(); more; more = reader.advance()) {
          c.output(reader.getCurrent());
        }
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<String>, PDone> {
    /** The prefix of each file written, combined with suffix and shardTemplate. */
    @Nullable abstract ValueProvider<ResourceId> getFilenamePrefix();

    /** The suffix of each file written, combined with prefix and shardTemplate. */
    @Nullable abstract String getFilenameSuffix();

    /** An optional header to add to each file. */
    @Nullable abstract String getHeader();

    /** An optional footer to add to each file. */
    @Nullable abstract String getFooter();

    /** Requested number of shards. 0 for automatic. */
    abstract int getNumShards();

    /** The shard template of each file written, combined with prefix and suffix. */
    @Nullable abstract String getShardTemplate();

    /** A policy for naming output files. */
    @Nullable abstract FilenamePolicy getFilenamePolicy();

    /** Whether to write windowed output files. */
    abstract boolean getWindowedWrites();

    /**
     * The {@link WritableByteChannelFactory} to be used by the {@link FileBasedSink}. Default is
     * {@link FileBasedSink.CompressionType#UNCOMPRESSED}.
     */
    abstract WritableByteChannelFactory getWritableByteChannelFactory();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilenamePrefix(ValueProvider<ResourceId> filenamePrefix);
      abstract Builder setShardTemplate(@Nullable String shardTemplate);
      abstract Builder setFilenameSuffix(@Nullable String filenameSuffix);
      abstract Builder setHeader(@Nullable String header);
      abstract Builder setFooter(@Nullable String footer);
      abstract Builder setFilenamePolicy(@Nullable FilenamePolicy filenamePolicy);
      abstract Builder setNumShards(int numShards);
      abstract Builder setWindowedWrites(boolean windowedWrites);
      abstract Builder setWritableByteChannelFactory(
          WritableByteChannelFactory writableByteChannelFactory);

      abstract Write build();
    }

    /**
     * Writes to text files with the given prefix. The given {@code prefix} can reference any
     * {@link FileSystem} on the classpath.
     *
     * <p>The name of the output files will be determined by the {@link FilenamePolicy} used.
     *
     * <p>By default, a {@link DefaultFilenamePolicy} will be used built using the specified prefix
     * to define the base output directory and file prefix, a shard identifier (see
     * {@link #withNumShards(int)}), and a common suffix (if supplied using
     * {@link #withSuffix(String)}).
     *
     * <p>This default policy can be overridden using {@link #withFilenamePolicy(FilenamePolicy)},
     * in which case {@link #withShardNameTemplate(String)} and {@link #withSuffix(String)} should
     * not be set.
     */
    public Write to(String filenamePrefix) {
      return to(FileBasedSink.convertToFileResourceIfPossible(filenamePrefix));
    }

    /**
     * Writes to text files with prefix from the given resource.
     *
     * <p>The name of the output files will be determined by the {@link FilenamePolicy} used.
     *
     * <p>By default, a {@link DefaultFilenamePolicy} will be used built using the specified prefix
     * to define the base output directory and file prefix, a shard identifier (see
     * {@link #withNumShards(int)}), and a common suffix (if supplied using
     * {@link #withSuffix(String)}).
     *
     * <p>This default policy can be overridden using {@link #withFilenamePolicy(FilenamePolicy)},
     * in which case {@link #withShardNameTemplate(String)} and {@link #withSuffix(String)} should
     * not be set.
     */
    @Experimental(Kind.FILESYSTEM)
    public Write to(ResourceId filenamePrefix) {
      return toResource(StaticValueProvider.of(filenamePrefix));
    }

    /**
     * Like {@link #to(String)}.
     */
    public Write to(ValueProvider<String> outputPrefix) {
      return toResource(NestedValueProvider.of(outputPrefix,
          new SerializableFunction<String, ResourceId>() {
            @Override
            public ResourceId apply(String input) {
              return FileBasedSink.convertToFileResourceIfPossible(input);
            }
          }));
    }

    /**
     * Like {@link #to(ResourceId)}.
     */
    @Experimental(Kind.FILESYSTEM)
    public Write toResource(ValueProvider<ResourceId> filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    /**
     * Uses the given {@link ShardNameTemplate} for naming output files. This option may only be
     * used when {@link #withFilenamePolicy(FilenamePolicy)} has not been configured.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public Write withShardNameTemplate(String shardTemplate) {
      return toBuilder().setShardTemplate(shardTemplate).build();
    }

    /**
     * Configures the filename suffix for written files. This option may only be used when
     * {@link #withFilenamePolicy(FilenamePolicy)} has not been configured.
     *
     * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
     * used.
     */
    public Write withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    /**
     * Configures the {@link FileBasedSink.FilenamePolicy} that will be used to name written files.
     */
    public Write withFilenamePolicy(FilenamePolicy filenamePolicy) {
      return toBuilder().setFilenamePolicy(filenamePolicy).build();
    }

    /**
     * Configures the number of output shards produced overall (when using unwindowed writes) or
     * per-window (when using windowed writes).
     *
     * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
     * performance of a pipeline. Setting this value is not recommended unless you require a
     * specific number of output files.
     *
     * @param numShards the number of shards to use, or 0 to let the system decide.
     */
    public Write withNumShards(int numShards) {
      checkArgument(numShards >= 0);
      return toBuilder().setNumShards(numShards).build();
    }

    /**
     * Forces a single file as output and empty shard name template. This option is only compatible
     * with unwindowed writes.
     *
     * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
     * performance of a pipeline. Setting this value is not recommended unless you require a
     * specific number of output files.
     *
     * <p>This is equivalent to {@code .withNumShards(1).withShardNameTemplate("")}
     */
    public Write withoutSharding() {
      return withNumShards(1).withShardNameTemplate("");
    }

    /**
     * Adds a header string to each file. A newline after the header is added automatically.
     *
     * <p>A {@code null} value will clear any previously configured header.
     */
    public Write withHeader(@Nullable String header) {
      return toBuilder().setHeader(header).build();
    }

    /**
     * Adds a footer string to each file. A newline after the footer is added automatically.
     *
     * <p>A {@code null} value will clear any previously configured footer.
     */
    public Write withFooter(@Nullable String footer) {
      return toBuilder().setFooter(footer).build();
    }

    /**
     * Returns a transform for writing to text files like this one but that has the given
     * {@link WritableByteChannelFactory} to be used by the {@link FileBasedSink} during output.
     * The default is value is {@link FileBasedSink.CompressionType#UNCOMPRESSED}.
     *
     * <p>A {@code null} value will reset the value to the default value mentioned above.
     */
    public Write withWritableByteChannelFactory(
        WritableByteChannelFactory writableByteChannelFactory) {
      return toBuilder().setWritableByteChannelFactory(writableByteChannelFactory).build();
    }

    public Write withWindowedWrites() {
      return toBuilder().setWindowedWrites(true).build();
    }

    @Override
    public PDone expand(PCollection<String> input) {
      checkState(getFilenamePrefix() != null,
          "Need to set the filename prefix of a TextIO.Write transform.");
      checkState(
          (getFilenamePolicy() == null)
              || (getShardTemplate() == null && getFilenameSuffix() == null),
          "Cannot set a filename policy and also a filename template or suffix.");

      FilenamePolicy usedFilenamePolicy = getFilenamePolicy();
      if (usedFilenamePolicy == null) {
        usedFilenamePolicy = DefaultFilenamePolicy.constructUsingStandardParameters(
            getFilenamePrefix(), getShardTemplate(), getFilenameSuffix(), getWindowedWrites());
      }
      WriteFiles<String> write =
          WriteFiles.to(
              new TextSink(
                  getFilenamePrefix(),
                  usedFilenamePolicy,
                  getHeader(),
                  getFooter(),
                  getWritableByteChannelFactory()));
      if (getNumShards() > 0) {
        write = write.withNumShards(getNumShards());
      }
      if (getWindowedWrites()) {
        write = write.withWindowedWrites();
      }
      return input.apply("WriteFiles", write);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      String prefixString = "";
      if (getFilenamePrefix() != null) {
        prefixString = getFilenamePrefix().isAccessible()
            ? getFilenamePrefix().get().toString() : getFilenamePrefix().toString();
      }
      builder
          .addIfNotNull(DisplayData.item("filePrefix", prefixString)
            .withLabel("Output File Prefix"))
          .addIfNotNull(DisplayData.item("fileSuffix", getFilenameSuffix())
            .withLabel("Output File Suffix"))
          .addIfNotNull(DisplayData.item("shardNameTemplate", getShardTemplate())
            .withLabel("Output Shard Name Template"))
          .addIfNotDefault(DisplayData.item("numShards", getNumShards())
            .withLabel("Maximum Output Shards"), 0)
          .addIfNotNull(DisplayData.item("fileHeader", getHeader())
            .withLabel("File Header"))
          .addIfNotNull(DisplayData.item("fileFooter", getFooter())
              .withLabel("File Footer"))
          .add(DisplayData
              .item("writableByteChannelFactory", getWritableByteChannelFactory().toString())
              .withLabel("Compression/Transformation Type"));
    }

    @Override
    protected Coder<Void> getDefaultOutputCoder() {
      return VoidCoder.of();
    }
  }

  /**
   * Possible text file compression types.
   */
  public enum CompressionType {
    /**
     * Automatically determine the compression type based on filename extension.
     */
    AUTO(""),
    /**
     * Uncompressed (i.e., may be split).
     */
    UNCOMPRESSED(""),
    /**
     * GZipped.
     */
    GZIP(".gz"),
    /**
     * BZipped.
     */
    BZIP2(".bz2"),
    /**
     * Zipped.
     */
    ZIP(".zip"),
    /**
     * Deflate compressed.
     */
    DEFLATE(".deflate");

    private String filenameSuffix;

    CompressionType(String suffix) {
      this.filenameSuffix = suffix;
    }

    /**
     * Determine if a given filename matches a compression type based on its extension.
     * @param filename the filename to match
     * @return true iff the filename ends with the compression type's known extension.
     */
    public boolean matches(String filename) {
      return filename.toLowerCase().endsWith(filenameSuffix.toLowerCase());
    }
  }

  //////////////////////////////////////////////////////////////////////////////

  /** Disable construction of utility class. */
  private TextIO() {}
}
