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
package org.apache.beam.sdk.io.synthetic;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.math3.stat.StatUtils.sum;

import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.OffsetBasedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions.ProgressShape;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link SyntheticBoundedIO} class provides a parameterizable batch custom source that is
 * deterministic.
 *
 * <p>The {@link SyntheticBoundedSource} generates a {@link PCollection} of {@code KV<byte[],
 * byte[]>}. A fraction of the generated records {@code KV<byte[], byte[]>} are associated with
 * "hot" keys, which are uniformly distributed over a fixed number of hot keys. The remaining
 * generated records are associated with "random" keys. Each record will be slowed down by a certain
 * sleep time generated based on the specified sleep time distribution when the {@link
 * SyntheticSourceReader} reads each record. The record {@code KV<byte[], byte[]>} is generated
 * deterministically based on the record's position in the source, which enables repeatable
 * execution for debugging. The SyntheticBoundedInput configurable parameters are defined in {@link
 * SyntheticSourceOptions}.
 *
 * <p>To read a {@link PCollection} of {@code KV<byte[], byte[]>} from {@link SyntheticBoundedIO},
 * use {@link SyntheticBoundedIO#readFrom} to construct the synthetic source with synthetic source
 * options. See {@link SyntheticSourceOptions} for how to construct an instance. An example is
 * below:
 *
 * <pre>{@code
 * Pipeline p = ...;
 * SyntheticBoundedInput.SourceOptions sso = ...;
 *
 * // Construct the synthetic input with synthetic source options.
 * PCollection<KV<byte[], byte[]>> input = p.apply(SyntheticBoundedInput.readFrom(sso));
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SyntheticBoundedIO {
  /** Read from the synthetic source options. */
  public static Read.Bounded<KV<byte[], byte[]>> readFrom(SyntheticSourceOptions options) {
    checkNotNull(options, "Input synthetic source options should not be null.");
    return Read.from(new SyntheticBoundedSource(options));
  }

  /** A {@link SyntheticBoundedSource} that reads {@code KV<byte[], byte[]>}. */
  public static class SyntheticBoundedSource extends OffsetBasedSource<KV<byte[], byte[]>> {
    private static final long serialVersionUID = 0;
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticBoundedSource.class);

    private final SyntheticSourceOptions sourceOptions;

    public SyntheticBoundedSource(SyntheticSourceOptions sourceOptions) {
      this(0, sourceOptions.numRecords, sourceOptions);
    }

    SyntheticBoundedSource(long startOffset, long endOffset, SyntheticSourceOptions sourceOptions) {
      super(startOffset, endOffset, 1);
      this.sourceOptions = sourceOptions;
      LOG.debug("Constructing {}", toString());
    }

    @Override
    public Coder<KV<byte[], byte[]>> getDefaultOutputCoder() {
      return KvCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of());
    }

    @Override
    // TODO: test cases where the source size could not be estimated (i.e., return 0).
    // TODO: test cases where the key size and value size might differ from record to record.
    // The key size and value size might have their own distributions.
    public long getBytesPerOffset() {
      return sourceOptions.bytesPerRecord >= 0
          ? sourceOptions.bytesPerRecord
          : sourceOptions.keySizeBytes + sourceOptions.valueSizeBytes;
    }

    @Override
    public void validate() {
      super.validate();
      sourceOptions.validate();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("options", sourceOptions)
          .add("offsetRange", "[" + getStartOffset() + ", " + getEndOffset() + ")")
          .toString();
    }

    @Override
    public final SyntheticBoundedSource createSourceForSubrange(long start, long end) {
      checkArgument(
          start >= getStartOffset(),
          "Start offset value "
              + start
              + " of the subrange cannot be smaller than the start offset value "
              + getStartOffset()
              + " of the parent source");
      checkArgument(
          end <= getEndOffset(),
          "End offset value "
              + end
              + " of the subrange cannot be larger than the end offset value "
              + getEndOffset()
              + " of the parent source");

      return new SyntheticBoundedSource(start, end, sourceOptions);
    }

    @Override
    public long getMaxEndOffset(PipelineOptions options) {
      return getEndOffset();
    }

    @Override
    public SyntheticSourceReader createReader(PipelineOptions pipelineOptions) {
      return new SyntheticSourceReader(this);
    }

    @Override
    public List<SyntheticBoundedSource> split(long desiredBundleSizeBytes, PipelineOptions options)
        throws Exception {
      // Choose number of bundles either based on explicit parameter,
      // or based on size and hints.
      int desiredNumBundles =
          (sourceOptions.forceNumInitialBundles == null)
              ? ((int) Math.ceil(1.0 * getEstimatedSizeBytes(options) / desiredBundleSizeBytes))
              : sourceOptions.forceNumInitialBundles;

      List<SyntheticBoundedSource> res =
          generateBundleSizes(desiredNumBundles)
              .stream()
              .map(
                  offsetRange ->
                      createSourceForSubrange(offsetRange.getFrom(), offsetRange.getTo()))
              .collect(Collectors.toList());
      LOG.info("Split into {} bundles of sizes: {}", res.size(), res);
      return res;
    }

    private List<OffsetRange> generateBundleSizes(int desiredNumBundles) {
      List<OffsetRange> result = new ArrayList<>();

      // Generate relative bundle sizes using the given distribution.
      double[] relativeSizes = new double[desiredNumBundles];
      for (int i = 0; i < relativeSizes.length; ++i) {
        relativeSizes[i] =
            sourceOptions.bundleSizeDistribution.sample(
                sourceOptions.hashFunction().hashInt(i).asLong());
      }

      // Generate offset ranges proportional to the relative sizes.
      double s = sum(relativeSizes);
      long startOffset = getStartOffset();
      double sizeSoFar = 0;
      for (int i = 0; i < relativeSizes.length; ++i) {
        sizeSoFar += relativeSizes[i];
        long endOffset =
            (i == relativeSizes.length - 1)
                ? getEndOffset()
                : (long) (getStartOffset() + sizeSoFar * (getEndOffset() - getStartOffset()) / s);
        if (startOffset != endOffset) {
          result.add(new OffsetRange(startOffset, endOffset));
        }
        startOffset = endOffset;
      }
      return result;
    }
  }

  /**
   * A reader over the {@link PCollection} of {@code KV<byte[], byte[]>} from the synthetic source.
   *
   * <p>The random but deterministic record at position "i" in the range [A, B) is generated by
   * using {@link SyntheticSourceOptions#genRecord}. Reading each record sleeps according to the
   * sleep time distribution in {@code SyntheticOptions}.
   */
  private static class SyntheticSourceReader
      extends OffsetBasedSource.OffsetBasedReader<KV<byte[], byte[]>> {
    private final long splitPointFrequencyRecords;

    private KV<byte[], byte[]> currentKvPair;
    private long currentOffset;
    private boolean isAtSplitPoint;

    SyntheticSourceReader(SyntheticBoundedSource source) {
      super(source);
      this.currentKvPair = null;
      this.splitPointFrequencyRecords = source.sourceOptions.splitPointFrequencyRecords;
    }

    @Override
    public synchronized SyntheticBoundedSource getCurrentSource() {
      return (SyntheticBoundedSource) super.getCurrentSource();
    }

    @Override
    protected long getCurrentOffset() throws IllegalStateException {
      return currentOffset;
    }

    @Override
    public KV<byte[], byte[]> getCurrent() throws NoSuchElementException {
      if (currentKvPair == null) {
        throw new NoSuchElementException(
            "The current element is unavailable because either the reader is "
                + "at the beginning of the input and start() or advance() wasn't called, "
                + "or the last start() or advance() returned false.");
      }
      return currentKvPair;
    }

    @Override
    public boolean allowsDynamicSplitting() {
      return splitPointFrequencyRecords > 0;
    }

    @Override
    protected final boolean startImpl() throws IOException {
      this.currentOffset = getCurrentSource().getStartOffset();
      if (splitPointFrequencyRecords > 0) {
        while (currentOffset % splitPointFrequencyRecords != 0) {
          ++currentOffset;
        }
      }

      SyntheticSourceOptions options = getCurrentSource().sourceOptions;
      SyntheticUtils.delay(
          options.nextInitializeDelay(this.currentOffset),
          options.cpuUtilizationInMixedDelay,
          options.delayType,
          new Random(this.currentOffset));

      isAtSplitPoint = true;
      --currentOffset;
      return advanceImpl();
    }

    @Override
    protected boolean advanceImpl() {
      currentOffset++;
      isAtSplitPoint =
          (splitPointFrequencyRecords == 0) || (currentOffset % splitPointFrequencyRecords == 0);

      SyntheticSourceOptions options = getCurrentSource().sourceOptions;
      SyntheticSourceOptions.Record record = options.genRecord(currentOffset);
      currentKvPair = record.kv;
      // TODO: add a separate distribution for the sleep time of reading the first record
      // (e.g.,"open" the files).
      long hashCodeOfVal = options.hashFunction().hashBytes(currentKvPair.getValue()).asLong();
      Random random = new Random(hashCodeOfVal);
      SyntheticUtils.delay(
          record.sleepMsec, options.cpuUtilizationInMixedDelay, options.delayType, random);

      return true;
    }

    @Override
    public void close() {
      // Nothing
    }

    @Override
    public Double getFractionConsumed() {
      double realFractionConsumed = super.getFractionConsumed();
      ProgressShape shape = getCurrentSource().sourceOptions.progressShape;
      switch (shape) {
        case LINEAR:
          return realFractionConsumed;
        case LINEAR_REGRESSING:
          return 0.9 - 0.8 * realFractionConsumed;
        default:
          throw new AssertionError("Unexpected progress shape: " + shape);
      }
    }

    @Override
    protected boolean isAtSplitPoint() throws NoSuchElementException {
      return isAtSplitPoint;
    }
  }
}
