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
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms for working with files. Currently includes matching of filepatterns via {@link #match}
 * and {@link #matchAll}.
 */
public class FileIO {
  private static final Logger LOG = LoggerFactory.getLogger(FileIO.class);

  /**
   * Matches a filepattern using {@link FileSystems#match} and produces a collection of matched
   * resources (both files and directories) as {@link MatchResult.Metadata}.
   *
   * <p>By default, matches the filepattern once and produces a bounded {@link PCollection}. To
   * continuously watch the filepattern for new matches, use {@link MatchAll#continuously(Duration,
   * TerminationCondition)} - this will produce an unbounded {@link PCollection}.
   *
   * <p>By default, a filepattern matching no resources is treated according to {@link
   * EmptyMatchTreatment#DISALLOW}. To configure this behavior, use {@link
   * Match#withEmptyMatchTreatment}.
   */
  public static Match match() {
    return new AutoValue_FileIO_Match.Builder()
        .setConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .build();
  }

  /**
   * Like {@link #match}, but matches each filepattern in a collection of filepatterns.
   *
   * <p>Resources are not deduplicated between filepatterns, i.e. if the same resource matches
   * multiple filepatterns, it will be produced multiple times.
   *
   * <p>By default, a filepattern matching no resources is treated according to {@link
   * EmptyMatchTreatment#ALLOW_IF_WILDCARD}. To configure this behavior, use {@link
   * MatchAll#withEmptyMatchTreatment}.
   */
  public static MatchAll matchAll() {
    return new AutoValue_FileIO_MatchAll.Builder()
        .setConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
        .build();
  }

  public static <DestT, InputT> Write<DestT, InputT> write(FileSink<DestT, InputT> sink) {
    return new AutoValue_FileIO_Write.Builder<DestT, InputT>()
        .setSink(sink)
        .setCompression(Compression.UNCOMPRESSED)
        .setSideInputs(ImmutableList.<PCollectionView<?>>of())
        .setWindowedWrites(false)
        .build();
  }

  public static <InputT> Write<Void, InputT> writeTo(
      FileSink<Void, InputT> sink, Write.FilenamePolicy policy) {
    return write(sink)
        .to(SerializableFunctions.<InputT, Void>constant(null))
        .withDestinationCoder(VoidCoder.of())
        .withFilenamePolicy(
            SerializableFunctions.<Void, Write.FilenamePolicy>constant(policy));
  }

  /**
   * Describes configuration for matching filepatterns, such as {@link EmptyMatchTreatment}
   * and continuous watching for matching files.
   */
  @AutoValue
  public abstract static class MatchConfiguration implements HasDisplayData, Serializable {
    /** Creates a {@link MatchConfiguration} with the given {@link EmptyMatchTreatment}. */
    public static MatchConfiguration create(EmptyMatchTreatment emptyMatchTreatment) {
      return new AutoValue_FileIO_MatchConfiguration.Builder()
          .setEmptyMatchTreatment(emptyMatchTreatment)
          .build();
    }

    abstract EmptyMatchTreatment getEmptyMatchTreatment();
    @Nullable abstract Duration getWatchInterval();
    @Nullable abstract TerminationCondition<String, ?> getWatchTerminationCondition();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setEmptyMatchTreatment(EmptyMatchTreatment treatment);
      abstract Builder setWatchInterval(Duration watchInterval);
      abstract Builder setWatchTerminationCondition(TerminationCondition<String, ?> condition);
      abstract MatchConfiguration build();
    }

    /** Sets the {@link EmptyMatchTreatment}. */
    public MatchConfiguration withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return toBuilder().setEmptyMatchTreatment(treatment).build();
    }

    /**
     * Continuously watches for new files at the given interval until the given termination
     * condition is reached, where the input to the condition is the filepattern.
     */
    public MatchConfiguration continuously(
        Duration interval, TerminationCondition<String, ?> condition) {
      return toBuilder().setWatchInterval(interval).setWatchTerminationCondition(condition).build();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder
          .add(
              DisplayData.item("emptyMatchTreatment", getEmptyMatchTreatment().toString())
                  .withLabel("Treatment of filepatterns that match no files"))
          .addIfNotNull(
              DisplayData.item("watchForNewFilesInterval", getWatchInterval())
                  .withLabel("Interval to watch for new files"));
    }
  }

  /** Implementation of {@link #match}. */
  @AutoValue
  public abstract static class Match extends PTransform<PBegin, PCollection<MatchResult.Metadata>> {
    abstract ValueProvider<String> getFilepattern();
    abstract MatchConfiguration getConfiguration();
    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);
      abstract Builder setConfiguration(MatchConfiguration configuration);
      abstract Match build();
    }

    /** Matches the given filepattern. */
    public Match filepattern(String filepattern) {
      return this.filepattern(ValueProvider.StaticValueProvider.of(filepattern));
    }

    /** Like {@link #filepattern(String)} but using a {@link ValueProvider}. */
    public Match filepattern(ValueProvider<String> filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    /** Sets the {@link MatchConfiguration}. */
    public Match withConfiguration(MatchConfiguration configuration) {
      return toBuilder().setConfiguration(configuration).build();
    }

    /** See {@link MatchConfiguration#withEmptyMatchTreatment(EmptyMatchTreatment)}. */
    public Match withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withConfiguration(getConfiguration().withEmptyMatchTreatment(treatment));
    }

    /**
     * See {@link MatchConfiguration#continuously}. The returned {@link PCollection} is unbounded.
     *
     * <p>This works only in runners supporting {@link Experimental.Kind#SPLITTABLE_DO_FN}.
     */
    @Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
    public Match continuously(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withConfiguration(getConfiguration().continuously(pollInterval, terminationCondition));
    }

    @Override
    public PCollection<MatchResult.Metadata> expand(PBegin input) {
      return input
          .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
          .apply("Via MatchAll", matchAll().withConfiguration(getConfiguration()));
    }
  }

  /** Implementation of {@link #matchAll}. */
  @AutoValue
  public abstract static class MatchAll
      extends PTransform<PCollection<String>, PCollection<MatchResult.Metadata>> {
    abstract MatchConfiguration getConfiguration();
    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConfiguration(MatchConfiguration configuration);
      abstract MatchAll build();
    }

    /** Like {@link Match#withConfiguration}. */
    public MatchAll withConfiguration(MatchConfiguration configuration) {
      return toBuilder().setConfiguration(configuration).build();
    }

    /** Like {@link Match#withEmptyMatchTreatment}. */
    public MatchAll withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withConfiguration(getConfiguration().withEmptyMatchTreatment(treatment));
    }

    /** Like {@link Match#continuously}. */
    @Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
    public MatchAll continuously(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return withConfiguration(getConfiguration().continuously(pollInterval, terminationCondition));
    }

    @Override
    public PCollection<MatchResult.Metadata> expand(PCollection<String> input) {
      if (getConfiguration().getWatchInterval() == null) {
        return input.apply(
            "Match filepatterns",
            ParDo.of(new MatchFn(getConfiguration().getEmptyMatchTreatment())));
      } else {
        return input
            .apply(
                "Continuously match filepatterns",
                Watch.growthOf(new MatchPollFn())
                    .withPollInterval(getConfiguration().getWatchInterval())
                    .withTerminationPerInput(getConfiguration().getWatchTerminationCondition()))
            .apply(Values.<MatchResult.Metadata>create());
      }
    }

    private static class MatchFn extends DoFn<String, MatchResult.Metadata> {
      private final EmptyMatchTreatment emptyMatchTreatment;

      public MatchFn(EmptyMatchTreatment emptyMatchTreatment) {
        this.emptyMatchTreatment = emptyMatchTreatment;
      }

      @ProcessElement
      public void process(ProcessContext c) throws Exception {
        String filepattern = c.element();
        MatchResult match = FileSystems.match(filepattern, emptyMatchTreatment);
        LOG.info("Matched {} files for pattern {}", match.metadata().size(), filepattern);
        for (MatchResult.Metadata metadata : match.metadata()) {
          c.output(metadata);
        }
      }
    }

    private static class MatchPollFn implements Watch.Growth.PollFn<String, MatchResult.Metadata> {
      @Override
      public Watch.Growth.PollResult<MatchResult.Metadata> apply(String input, Instant timestamp)
          throws Exception {
        return Watch.Growth.PollResult.incomplete(
            Instant.now(), FileSystems.match(input, EmptyMatchTreatment.ALLOW).metadata());
      }
    }
  }

  /**
   * A {@link PTransform} that writes to a {@link FileBasedSink}. A write begins with a sequential
   * global initialization of a sink, followed by a parallel write, and ends with a sequential
   * finalization of the write. The output of a write is {@link PDone}.
   *
   * <p>By default, every bundle in the input {@link PCollection} will be processed by a {@link
   * FileBasedSink.WriteOperation}, so the number of output will vary based on runner behavior, though at least 1
   * output will always be produced. The exact parallelism of the write stage can be controlled using
   * {@link Write#withNumShards}, typically used to control how many files are produced or to
   * globally limit the number of workers connecting to an external service. However, this option can
   * often hurt performance: it adds an additional {@link GroupByKey} to the pipeline.
   *
   * <p>Example usage with runner-determined sharding:
   *
   * <pre>{@code p.apply(WriteFiles.to(new MySink(...)));}</pre>
   *
   * <p>Example usage with a fixed number of shards:
   *
   * <pre>{@code p.apply(WriteFiles.to(new MySink(...)).withNumShards(3));}</pre>
   */
  @AutoValue
  @Experimental(Experimental.Kind.SOURCE_SINK)
  public abstract static class Write<DestinationT, UserT>
      extends PTransform<PCollection<UserT>, WriteFilesResult<DestinationT>> {
    interface FilenameContext {
      BoundedWindow getWindow();

      PaneInfo getPane();

      int getShardIndex();

      int getNumShards();

      Compression getCompression();
    }

    interface FilenamePolicy extends Serializable {
      ResourceId getFilename(FilenameContext context);
    }

    public static FilenamePolicy toPrefixAndShardTemplate(
        final ResourceId prefix, final String shardTempate) {
      final DefaultFilenamePolicy policy =
          new DefaultFilenamePolicy(
              new DefaultFilenamePolicy.Params()
                  .withBaseFilename(prefix)
                  .withShardTemplate(shardTempate));
      return new FilenamePolicy() {
        @Override
        public ResourceId getFilename(final FilenameContext context) {
          return policy.windowedFilename(
              context.getShardIndex(),
              context.getNumShards(),
              context.getWindow(),
              context.getPane(),
              FileBasedSink.CompressionType.fromCanonical(context.getCompression()));
        }
      };
    }

    public static FilenamePolicy toPrefixAndWindowedShard(final ResourceId prefix) {
      return toPrefixAndShardTemplate(
          prefix, DefaultFilenamePolicy.DEFAULT_WINDOWED_SHARD_TEMPLATE);
    }

    public static FilenamePolicy toPrefixAndUnwindowedShard(final ResourceId prefix) {
      return toPrefixAndShardTemplate(
          prefix, DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE);
    }

    abstract FileSink<DestinationT, UserT> getSink();

    @Nullable
    abstract SerializableFunction<UserT, DestinationT> getDestinationFn();

    @Nullable
    abstract SerializableFunction<DestinationT, FilenamePolicy> getFilenamePolicyFn();

    @Nullable
    abstract DestinationT getEmptyWindowDestination();

    @Nullable
    abstract Coder<DestinationT> getDestinationCoder();

    @Nullable
    abstract ValueProvider<ResourceId> getTempDirectoryProvider();

    abstract Compression getCompression();

    abstract List<PCollectionView<?>> getSideInputs();

    @Nullable
    abstract ValueProvider<Integer> getNumShards();

    @Nullable
    abstract PTransform<PCollection<UserT>, PCollectionView<Integer>> getSharding();

    abstract boolean getWindowedWrites();

    abstract Builder<DestinationT, UserT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<DestinationT, UserT> {
      abstract Builder<DestinationT, UserT> setSink(FileSink<DestinationT, UserT> sink);

      abstract Builder<DestinationT, UserT> setDestinationFn(
          SerializableFunction<UserT, DestinationT> destinationFn);

      abstract Builder<DestinationT, UserT> setFilenamePolicyFn(
          SerializableFunction<DestinationT, FilenamePolicy> policyFn);

      abstract Builder<DestinationT, UserT> setEmptyWindowDestination(
          DestinationT emptyWindowDestination);

      abstract Builder<DestinationT, UserT> setDestinationCoder(Coder<DestinationT> destinationCoder);

      abstract Builder<DestinationT, UserT> setTempDirectoryProvider(
          ValueProvider<ResourceId> tempDirectoryProvider);

      abstract Builder<DestinationT, UserT> setCompression(Compression compression);

      abstract Builder<DestinationT, UserT> setSideInputs(List<PCollectionView<?>> sideInputs);

      abstract Builder<DestinationT, UserT> setNumShards(ValueProvider<Integer> numShards);

      abstract Builder<DestinationT, UserT> setSharding(
          PTransform<PCollection<UserT>, PCollectionView<Integer>> sharding);

      abstract Builder<DestinationT, UserT> setWindowedWrites(boolean windowedWrites);

      abstract Write<DestinationT, UserT> build();
    }

    public Write<DestinationT, UserT> to(
        SerializableFunction<UserT, DestinationT> destinationFn) {
      return toBuilder().setDestinationFn(destinationFn).build();
    }

    public Write<DestinationT, UserT> withFilenamePolicy(
        SerializableFunction<DestinationT, FilenamePolicy> policyFn) {
      return toBuilder().setFilenamePolicyFn(policyFn).build();
    }

    public Write<DestinationT, UserT> withTempDirectory(
        ResourceId tempDirectory) {
      return withTempDirectory(ValueProvider.StaticValueProvider.of(tempDirectory));
    }

    public Write<DestinationT, UserT> withTempDirectory(
        ValueProvider<ResourceId> tempDirectory) {
      return toBuilder().setTempDirectoryProvider(tempDirectory).build();
    }

    public Write<DestinationT, UserT> withEmptyGlobalWindowDestination(
        DestinationT emptyWindowDestination) {
      return toBuilder().setEmptyWindowDestination(emptyWindowDestination).build();
    }

    public Write<DestinationT, UserT> withDestinationCoder(
        Coder<DestinationT> destinationCoder) {
      return toBuilder().setDestinationCoder(destinationCoder).build();
    }

    public Write<DestinationT, UserT> withSideInputs(List<PCollectionView<?>> sideInputs) {
      return toBuilder().setSideInputs(sideInputs).build();
    }

    public Write<DestinationT, UserT> withSideInputs(PCollectionView<?>... sideInputs) {
      return toBuilder().setSideInputs(Arrays.asList(sideInputs)).build();
    }

    public Write<DestinationT, UserT> withNumShards(int numShards) {
      if (numShards == 0) {
        return withNumShards(null);
      }
      return withNumShards(ValueProvider.StaticValueProvider.of(numShards));
    }

    public Write<DestinationT, UserT> withNumShards(ValueProvider<Integer> numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    public Write<DestinationT, UserT> withWindowedWrites() {
      return toBuilder().setWindowedWrites(true).build();
    }

    @Override
    public WriteFilesResult<DestinationT> expand(PCollection<UserT> input) {
      Coder<DestinationT> destinationCoder = getDestinationCoder();
      if (destinationCoder == null) {
        TypeDescriptor<DestinationT> destinationT = TypeDescriptors.outputOf(getDestinationFn());
        try {
          destinationCoder =
              input
                  .getPipeline()
                  .getCoderRegistry()
                  .getCoder(destinationT);
        } catch (CannotProvideCoderException e) {
          throw new IllegalArgumentException(
              "Unable to infer a coder for destination type "
                  + destinationT
                  + " - specify it explicitly using .withDestinationCoder()");
        }
      }
      WriteFiles<UserT, DestinationT, UserT> writeFiles =
          WriteFiles.to(new ViaFileBasedSink<>(this, destinationCoder)).withSideInputs(getSideInputs());
      if (getNumShards() != null) {
        writeFiles = writeFiles.withNumShards(getNumShards());
      } else if (getSharding() != null) {
        writeFiles = writeFiles.withSharding(getSharding());
      } else {
        writeFiles = writeFiles.withRunnerDeterminedSharding();
      }
      if (getWindowedWrites()) {
        writeFiles = writeFiles.withWindowedWrites();
      }
      return input.apply(writeFiles);
    }

    private static class WindowedFilenameContext implements FilenameContext {
      private final BoundedWindow window;
      private final PaneInfo paneInfo;
      private final int shardNumber;
      private final int numShards;
      private final Compression compression;

      public WindowedFilenameContext(
          BoundedWindow window,
          PaneInfo paneInfo,
          int shardNumber,
          int numShards,
          Compression compression) {
        this.window = window;
        this.paneInfo = paneInfo;
        this.shardNumber = shardNumber;
        this.numShards = numShards;
        this.compression = compression;
      }

      @Override
      public BoundedWindow getWindow() {
        return window;
      }

      @Override
      public PaneInfo getPane() {
        return paneInfo;
      }

      @Override
      public int getShardIndex() {
        return shardNumber;
      }

      @Override
      public int getNumShards() {
        return numShards;
      }

      @Override
      public Compression getCompression() {
        return compression;
      }
    }

    private static class ViaFileBasedSink<UserT, DestinationT>
        extends FileBasedSink<UserT, DestinationT, UserT> {
      private final Write<DestinationT, UserT> spec;

      private ViaFileBasedSink(
          Write<DestinationT, UserT> spec, Coder<DestinationT> destinationCoder) {
        super(
            spec.getTempDirectoryProvider(),
            new DynamicDestinationsAdapter<>(spec, destinationCoder));
        this.spec = spec;
      }

      @Override
      public WriteOperation<DestinationT, UserT> createWriteOperation() {
        return new WriteOperation<DestinationT, UserT>(this) {
          @Override
          public Writer<DestinationT, UserT> createWriter() throws Exception {
            return new Writer<DestinationT, UserT>(this, "") {
              private FileSink<DestinationT, UserT> sink = SerializableUtils.clone(spec.getSink());

              @Override
              protected void prepareWrite(WritableByteChannel channel) throws Exception {
                sink.open(getDestination(), channel);
              }

              @Override
              public void write(UserT value) throws Exception {
                sink.write(value);
              }

              @Override
              protected void finishWrite() throws Exception {
                sink.finish();
              }
            };
          }
        };
      }

      private static class DynamicDestinationsAdapter<DestinationT, UserT>
          extends DynamicDestinations<UserT, DestinationT, UserT> {
        private final Write<DestinationT, UserT> spec;
        private final Coder<DestinationT> destinationCoder;

        private DynamicDestinationsAdapter(
            Write<DestinationT, UserT> spec, Coder<DestinationT> destinationCoder) {
          this.spec = spec;
          this.destinationCoder = destinationCoder;
        }

        @Override
        public UserT formatRecord(UserT record) {
          return record;
        }

        @Override
        public DestinationT getDestination(UserT element) {
          return spec.getDestinationFn().apply(element);
        }

        @Override
        public DestinationT getDefaultDestination() {
          return spec.getEmptyWindowDestination();
        }

        @Override
        public FilenamePolicy getFilenamePolicy(final DestinationT destination) {
          final Write.FilenamePolicy policyFn = spec.getFilenamePolicyFn().apply(destination);
          return new FilenamePolicy() {
            @Override
            public ResourceId windowedFilename(
                int shardNumber,
                int numShards,
                BoundedWindow window,
                PaneInfo paneInfo,
                OutputFileHints outputFileHints) {
              // We ignore outputFileHints because it will always be the same as spec.getCompression()
              // because we control the FileBasedSink.
              return policyFn.getFilename(
                  new WindowedFilenameContext(
                      window, paneInfo, shardNumber, numShards, spec.getCompression()));
            }

            @Nullable
            @Override
            public ResourceId unwindowedFilename(
                int shardNumber, int numShards, OutputFileHints outputFileHints) {
              return policyFn.getFilename(
                  new WindowedFilenameContext(
                      GlobalWindow.INSTANCE,
                      PaneInfo.NO_FIRING,
                      shardNumber,
                      numShards,
                      spec.getCompression()));
            }
          };
        }

        @Override
        public List<PCollectionView<?>> getSideInputs() {
          return spec.getSideInputs();
        }

        @Nullable
        @Override
        public Coder<DestinationT> getDestinationCoder() {
          return destinationCoder;
        }
      }
    }
  }
}
