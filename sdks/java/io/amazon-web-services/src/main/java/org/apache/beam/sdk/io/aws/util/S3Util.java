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
package org.apache.beam.sdk.io.aws.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.aws.s3.S3Path;
import org.apache.beam.sdk.io.aws.s3.S3ResourceId;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3Util wraps the Amazon Web Services client library.
 */
public class S3Util {

  private static final Logger LOG = LoggerFactory.getLogger(S3Util.class);

  /**
   * This is a {@link DefaultValueFactory} able to create an {@link S3Util} using any transport
   * flags specified on the {@link PipelineOptions}.
   */
  public static class S3UtilFactory implements DefaultValueFactory<S3Util> {

    @Override
    public S3Util create(PipelineOptions options) {
      S3Options s3Options = options.as(S3Options.class);

      checkArgument(
          !Strings.isNullOrEmpty(s3Options.getAwsAccessKeyId()), "AWS access key ID is required");
      checkArgument(
          !Strings.isNullOrEmpty(s3Options.getAwsSecretAccessKey()),
          "AWS secret access key is required");
      checkArgument(!Strings.isNullOrEmpty(s3Options.getAwsRegion()), "AWS region is required");

      return new S3Util(
          s3Options.getAwsAccessKeyId(),
          s3Options.getAwsSecretAccessKey(),
          s3Options.getAwsRegion(),
          s3Options.getS3StorageClass(),
          s3Options.getS3UploadBufferSizeBytes(),
          s3Options.getS3ThreadPoolSize());
    }
  }

  // Amazon S3 API docs: Each part must be at least 5 MB in size, except the last part.
  private static final int MINIMUM_UPLOAD_BUFFER_SIZE_BYTES = 5 * 1024 * 1024;
  private static final int DEFAULT_UPLOAD_BUFFER_SIZE_BYTES =
      Runtime.getRuntime().maxMemory() < 512 * 1024 * 1024
          ? MINIMUM_UPLOAD_BUFFER_SIZE_BYTES
          : 64 * 1024 * 1024;
  private static final int MAX_THREADS_PER_CONCURRENT_COPY = 3;

  // Non-final for testing.
  private AmazonS3 amazonS3;
  private final String storageClass;
  private final int s3UploadBufferSizeBytes;
  private final ExecutorService executorService;

  private S3Util(
      String awsAccessKeyId,
      String awsSecretAccessKey,
      String region,
      String storageClass,
      Integer uploadBufferSizeBytes,
      int threadPoolSize) {
    AWSStaticCredentialsProvider credentialsProvider =
        new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey));
    amazonS3 =
        AmazonS3ClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion(region)
            .build();

    this.storageClass = checkNotNull(storageClass, "storageClass");

    if (uploadBufferSizeBytes == null) {
      uploadBufferSizeBytes = DEFAULT_UPLOAD_BUFFER_SIZE_BYTES;
    }
    this.s3UploadBufferSizeBytes =
        Math.max(MINIMUM_UPLOAD_BUFFER_SIZE_BYTES, uploadBufferSizeBytes);

    checkArgument(threadPoolSize > 0, "threadPoolSize");
    this.executorService = Executors.newFixedThreadPool(threadPoolSize);
  }

  @VisibleForTesting
  public void setAmazonS3Client(AmazonS3 amazonS3) {
    this.amazonS3 = amazonS3;
  }

  @VisibleForTesting
  public int getS3UploadBufferSizeBytes() {
    return s3UploadBufferSizeBytes;
  }

  @Override
  protected void finalize() {
    executorService.shutdownNow();
  }

  /**
   * Calls executorService.invokeAll() with tasks, then unwraps the resulting {@link Future
   * Futures}.
   *
   * <p>Any task exception is wrapped in {@link IOException}.
   */
  private static <T> List<T> invokeAllAndUnwrapResults(
      Collection<Callable<T>> tasks, ExecutorService executorService) throws IOException {
    List<Future<T>> futures;
    try {
      futures = executorService.invokeAll(tasks);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("executor service task was interrupted");
    }

    List<T> results = new ArrayList<>(tasks.size());
    try {
      for (Future<T> future : futures) {
        results.add(future.get());
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("executor service future.get() was interrupted");

    } catch (ExecutionException e) {
      if (e.getCause() != null) {
        if (e.getCause() instanceof IOException) {
          throw ((IOException) e.getCause());
        }
        throw new IOException(e.getCause());
      }
      throw new IOException(e);
    }

    return results;
  }

  public WritableByteChannel create(S3Path path) throws IOException {
    return new S3WritableByteChannel(amazonS3, path, storageClass, s3UploadBufferSizeBytes);
  }

  public ReadableByteChannel open(S3Path path) throws IOException {
    return new S3ReadableSeekableByteChannel(amazonS3, path);
  }

  public void copy(List<S3Path> sourcePaths, List<S3Path> destinationPaths) throws IOException {
    checkArgument(
        sourcePaths.size() == destinationPaths.size(),
        "sizes of sourcePaths and destinationPaths do not match");

    List<Callable<Void>> tasks = new ArrayList<>(sourcePaths.size());

    Iterator<S3Path> sourcePathsIterator = sourcePaths.iterator();
    Iterator<S3Path> destinationPathsIterator = destinationPaths.iterator();
    while (sourcePathsIterator.hasNext()) {
      final S3Path sourcePath = sourcePathsIterator.next();
      final S3Path destinationPath = destinationPathsIterator.next();

      tasks.add(
          new Callable<Void>() {
            @Override
            public Void call() throws IOException {
              copy(sourcePath, destinationPath);
              return null;
            }
          });
    }

    invokeAllAndUnwrapResults(tasks, executorService);
  }

  @VisibleForTesting
  void copy(S3Path sourcePath, S3Path destinationPath) throws IOException {
    InitiateMultipartUploadRequest initiateUploadRequest =
        new InitiateMultipartUploadRequest(destinationPath.getBucket(), destinationPath.getKey())
            .withStorageClass(storageClass);
    String uploadId;
    long objectSize;
    try {
      InitiateMultipartUploadResult initiateUploadResult =
          amazonS3.initiateMultipartUpload(initiateUploadRequest);
      uploadId = initiateUploadResult.getUploadId();

      ObjectMetadata objectMetadata =
          amazonS3.getObjectMetadata(sourcePath.getBucket(), sourcePath.getKey());
      objectSize = objectMetadata.getContentLength();

    } catch (AmazonClientException e) {
      throw new IOException(e);
    }

    List<Callable<PartETag>> tasks =
        new ArrayList<>((int) Math.ceil((double) objectSize / (double) s3UploadBufferSizeBytes));

    long bytePosition = 0;

    // Amazon parts are 1-indexed, not zero-indexed.
    for (int partNumber = 1; bytePosition < objectSize; partNumber++) {
      final CopyPartRequest copyPartRequest =
          new CopyPartRequest()
              .withSourceBucketName(sourcePath.getBucket())
              .withSourceKey(sourcePath.getKey())
              .withDestinationBucketName(destinationPath.getBucket())
              .withDestinationKey(destinationPath.getKey())
              .withUploadId(uploadId)
              .withPartNumber(partNumber)
              .withFirstByte(bytePosition)
              .withLastByte(Math.min(objectSize - 1, bytePosition + s3UploadBufferSizeBytes - 1));

      tasks.add(
          new Callable<PartETag>() {
            @Override
            public PartETag call() throws IOException {
              CopyPartResult copyPartResult;
              try {
                copyPartResult = amazonS3.copyPart(copyPartRequest);
              } catch (AmazonClientException e) {
                throw new IOException(e);
              }
              return copyPartResult.getPartETag();
            }
          });

      bytePosition += s3UploadBufferSizeBytes;
    }

    ExecutorService executorService;
    if (tasks.size() > 1) {
      // Don't pollute the main thread pool, which this was called from.
      // Instead, create a small thread pool just for this copy operation.
      int threadQuantity = Math.min(MAX_THREADS_PER_CONCURRENT_COPY, tasks.size());
      executorService = Executors.newFixedThreadPool(threadQuantity);
    } else {
      // Don't create a thread pool if there is only one task.
      executorService = MoreExecutors.newDirectExecutorService();
    }
    List<PartETag> eTags;
    try {
      eTags = invokeAllAndUnwrapResults(tasks, executorService);
    } finally {
      executorService.shutdown();
    }

    CompleteMultipartUploadRequest completeUploadRequest =
        new CompleteMultipartUploadRequest()
            .withBucketName(destinationPath.getBucket())
            .withKey(destinationPath.getKey())
            .withUploadId(uploadId)
            .withPartETags(eTags);

    try {
      amazonS3.completeMultipartUpload(completeUploadRequest);
    } catch (AmazonClientException e) {
      throw new IOException(e);
    }
  }

  public void delete(List<S3Path> paths) throws IOException {
    Multimap<String, String> keysByBucket = ArrayListMultimap.create();
    for (S3Path path : paths) {
      keysByBucket.put(path.getBucket(), path.getKey());
    }

    List<Callable<Void>> tasks = new ArrayList<>();
    for (final String bucket : keysByBucket.keySet()) {
      for (final List<String> keysPartition : Iterables.partition(keysByBucket.get(bucket), 1000)) {
        tasks.add(
            new Callable<Void>() {
              @Override
              public Void call() throws IOException {
                delete(bucket, keysPartition);
                return null;
              }
            });
      }
    }

    invokeAllAndUnwrapResults(tasks, executorService);
  }

  private void delete(String bucket, Collection<String> keys) throws IOException {
    // S3 SDK: "You may specify up to 1000 keys."
    checkArgument(keys.size() <= 1000, "only 1000 keys can be deleted per request");
    List<KeyVersion> deleteKeyVersions =
        FluentIterable.from(keys)
            .transform(
                new Function<String, KeyVersion>() {
                  @Override
                  public KeyVersion apply(String key) {
                    return new KeyVersion(key);
                  }
                })
            .toList();
    DeleteObjectsRequest request = new DeleteObjectsRequest(bucket).withKeys(deleteKeyVersions);
    try {
      amazonS3.deleteObjects(request);
    } catch (AmazonClientException e) {
      throw new IOException(e);
    }
  }

  public List<MatchResult> match(Collection<S3Path> paths) throws IOException {
    List<S3Path> globs = Lists.newArrayList();
    List<S3Path> nonGlobs = Lists.newArrayList();
    List<Boolean> isGlobBooleans = Lists.newArrayList();

    for (S3Path path : paths) {
      if (path.isWildcard()) {
        globs.add(path);
        isGlobBooleans.add(true);
      } else {
        nonGlobs.add(path);
        isGlobBooleans.add(false);
      }
    }

    Iterator<MatchResult> globMatches = matchGlobPaths(globs).iterator();
    Iterator<MatchResult> nonGlobMatches = matchNonGlobPaths(nonGlobs).iterator();

    ImmutableList.Builder<MatchResult> matchResults = ImmutableList.builder();
    for (Boolean isGlob : isGlobBooleans) {
      if (isGlob) {
        checkState(globMatches.hasNext(), "Expect globMatches has next.");
        matchResults.add(globMatches.next());
      } else {
        checkState(nonGlobMatches.hasNext(), "Expect nonGlobMatches has next.");
        matchResults.add(nonGlobMatches.next());
      }
    }
    checkState(!globMatches.hasNext(), "Expect no more elements in globMatches.");
    checkState(!nonGlobMatches.hasNext(), "Expect no more elements in nonGlobMatches.");

    return matchResults.build();
  }

  private List<MatchResult> matchGlobPaths(Collection<S3Path> paths) throws IOException {
    List<Callable<MatchResult>> tasks = new ArrayList<>(paths.size());
    for (final S3Path path : paths) {
      tasks.add(
          new Callable<MatchResult>() {
            @Override
            public MatchResult call() {
              return matchGlobPath(path);
            }
          });
    }

    return invokeAllAndUnwrapResults(tasks, executorService);
  }

  /**
   * Gets {@link MatchResult} representing all objects that match wildcard-containing path.
   */
  @VisibleForTesting
  MatchResult matchGlobPath(S3Path path) {
    // The S3 API can list objects, filtered by prefix, but not by wildcard.
    // Here, we find the longest prefix without wildcard "*",
    // then filter the results with a regex.
    checkArgument(path.isWildcard(), "isWildcard");
    String keyPrefix = path.getKeyNonWildcardPrefix();
    Pattern wildcardRegexp = Pattern.compile(S3Util.wildcardToRegexp(path.getKey()));

    LOG.debug(
        "matching files in bucket {}, prefix {} against pattern {}",
        path.getBucket(),
        keyPrefix,
        wildcardRegexp.toString());

    ImmutableList.Builder<MatchResult.Metadata> results = ImmutableList.builder();
    String continuationToken = null;

    do {
      ListObjectsV2Request request =
          new ListObjectsV2Request()
              .withBucketName(path.getBucket())
              .withPrefix(keyPrefix)
              .withContinuationToken(continuationToken);
      ListObjectsV2Result result;
      try {
        result = amazonS3.listObjectsV2(request);
      } catch (AmazonClientException e) {
        return MatchResult.create(MatchResult.Status.ERROR, new IOException(e));
      }
      continuationToken = result.getNextContinuationToken();

      for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
        // Filter against regex.
        if (wildcardRegexp.matcher(objectSummary.getKey()).matches()) {
          S3Path objectPath =
              S3Path.fromComponents(objectSummary.getBucketName(), objectSummary.getKey());
          LOG.debug("Matched S3 object: {}", objectPath);
          results.add(createBeamMetadata(objectPath, objectSummary.getSize()));
        }
      }
    } while (continuationToken != null);

    return MatchResult.create(MatchResult.Status.OK, results.build());
  }

  private List<MatchResult> matchNonGlobPaths(Collection<S3Path> paths) throws IOException {
    List<Callable<MatchResult>> tasks = new ArrayList<>(paths.size());
    for (final S3Path path : paths) {
      tasks.add(
          new Callable<MatchResult>() {
            @Override
            public MatchResult call() {
              return matchNonGlobPath(path);
            }
          });
    }

    return invokeAllAndUnwrapResults(tasks, executorService);
  }

  @VisibleForTesting
  MatchResult matchNonGlobPath(S3Path path) {
    ObjectMetadata s3Metadata;
    try {
      s3Metadata = amazonS3.getObjectMetadata(path.getBucket(), path.getKey());
    } catch (AmazonClientException e) {
      if (e instanceof AmazonS3Exception && ((AmazonS3Exception) e).getStatusCode() == 404) {
        return MatchResult.create(MatchResult.Status.NOT_FOUND, new FileNotFoundException());
      }
      return MatchResult.create(MatchResult.Status.ERROR, new IOException(e));
    }
    return MatchResult.create(
        MatchResult.Status.OK,
        ImmutableList.of(createBeamMetadata(path, s3Metadata.getContentLength())));
  }

  private static MatchResult.Metadata createBeamMetadata(S3Path path, long sizeBytes) {
    // TODO: Address https://issues.apache.org/jira/browse/BEAM-1494
    // It is incorrect to set IsReadSeekEfficient true for files with content encoding set to gzip.
    return MatchResult.Metadata.builder()
        .setIsReadSeekEfficient(true)
        .setResourceId(S3ResourceId.fromS3Path(path))
        .setSizeBytes(sizeBytes)
        .build();
  }

  /**
   * Expands glob expressions to regular expressions.
   *
   * @param globExp the glob expression to expand
   * @return a string with the regular expression this glob expands to
   */
  @VisibleForTesting
  static String wildcardToRegexp(String globExp) {
    StringBuilder dst = new StringBuilder();
    char[] src = globExp.replace("**/*", "**").toCharArray();
    int i = 0;
    while (i < src.length) {
      char c = src[i++];
      switch (c) {
        case '*':
          // One char lookahead for **
          if (i < src.length && src[i] == '*') {
            dst.append(".*");
            ++i;
          } else {
            dst.append("[^/]*");
          }
          break;
        case '?':
          dst.append("[^/]");
          break;
        case '.':
        case '+':
        case '{':
        case '}':
        case '(':
        case ')':
        case '|':
        case '^':
        case '$':
          // These need to be escaped in regular expressions
          dst.append('\\').append(c);
          break;
        case '\\':
          i = doubleSlashes(dst, src, i);
          break;
        default:
          dst.append(c);
          break;
      }
    }
    return dst.toString();
  }

  private static int doubleSlashes(StringBuilder dst, char[] src, int i) {
    // Emit the next character without special interpretation
    dst.append('\\');
    if ((i - 1) != src.length) {
      dst.append(src[i]);
      i++;
    } else {
      // A backslash at the very end is treated like an escaped backslash
      dst.append('\\');
    }
    return i;
  }
}
