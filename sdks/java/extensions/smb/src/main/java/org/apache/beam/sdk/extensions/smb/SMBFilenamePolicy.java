package org.apache.beam.sdk.extensions.smb;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;

public final class SMBFilenamePolicy implements Serializable {
  private static final String TEMPDIR_TIMESTAMP = "yyyy-MM-dd_HH-mm-ss";
  private static final String BEAM_TEMPDIR_PATTERN = ".temp-beam-%s";

  private final ResourceId filenamePrefix;
  private final String fileNameSuffix;

  public SMBFilenamePolicy(
      ResourceId destinationPrefix,
      String fileNameSuffix
  ) {
    this.filenamePrefix = destinationPrefix;
    this.fileNameSuffix = fileNameSuffix;
  }

  public FileAssignment forDestination() {
    return new FileAssignment(filenamePrefix, fileNameSuffix, false);
  }

  public FileAssignment forTempFiles(ResourceId tempDirectory) {
    return new FileAssignment(
        tempDirectory.resolve(
          String.format(
              BEAM_TEMPDIR_PATTERN,
              Instant.now().toString(DateTimeFormat.forPattern(TEMPDIR_TIMESTAMP))
          ), StandardResolveOptions.RESOLVE_DIRECTORY),
        fileNameSuffix,
        true);
  }

  public static class FileAssignment implements Serializable {
    private static final String BUCKET_TEMPLATE = "bucket-%d-of-%d";
    private static final String BUCKET_SHARD_TEMPLATE = BUCKET_TEMPLATE + "-shard-%d-of-%s.%s";
    private static final String METADATA_FILENAME = "metadata.json";
    private static final String TIMESTAMP_TEMPLATE = "yyyy-MM-dd_HH-mm-ss-";

    private final ResourceId filenamePrefix;
    private final String fileNameSuffix;
    private final boolean doTimestampFiles;

    FileAssignment(ResourceId filenamePrefix, String fileNameSuffix, boolean doTimestampFiles) {
      this.filenamePrefix = filenamePrefix;
      this.fileNameSuffix = fileNameSuffix;
      this.doTimestampFiles = doTimestampFiles;
    }

    FileAssignment(ResourceId filenamePrefix, String fileNameSuffix) {
      this(filenamePrefix, fileNameSuffix, false);
    }

    public ResourceId forBucketShard(int bucketNumber, int numBuckets, int shardNumber, int numShards) {
      String prefix = "";
      if (doTimestampFiles) {
        prefix += Instant.now().toString(DateTimeFormat.forPattern(TIMESTAMP_TEMPLATE));
      }

      return filenamePrefix.resolve(
          prefix + String.format(BUCKET_SHARD_TEMPLATE, bucketNumber, numBuckets, shardNumber, numShards, fileNameSuffix),
          StandardResolveOptions.RESOLVE_FILE
      );
    }

    List<ResourceId> forAllBucketShards(int bucketNumber, int numBuckets) {
      final List<ResourceId> resourceIds = new ArrayList<>();
      try {
        MatchResult matchResult = FileSystems
            .match(filenamePrefix + String.format(BUCKET_TEMPLATE, bucketNumber, numBuckets) + "*." + fileNameSuffix);

        matchResult.metadata().iterator()
            .forEachRemaining(metadata -> resourceIds.add(metadata.resourceId()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      return resourceIds;
    }

    ResourceId forMetadata() {
      return filenamePrefix.resolve(METADATA_FILENAME, StandardResolveOptions.RESOLVE_FILE);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FileAssignment that = (FileAssignment) o;
      return doTimestampFiles == that.doTimestampFiles &&
          Objects.equals(filenamePrefix, that.filenamePrefix) &&
          Objects.equals(fileNameSuffix, that.fileNameSuffix);
    }

    @Override
    public int hashCode() {
      return Objects.hash(filenamePrefix, fileNameSuffix, doTimestampFiles);
    }
  }
}
