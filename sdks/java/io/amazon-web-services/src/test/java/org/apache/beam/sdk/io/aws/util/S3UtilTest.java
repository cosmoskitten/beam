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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.aws.s3.S3Path;
import org.apache.beam.sdk.io.aws.s3.S3ResourceId;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

/**
 * Test case for {@link S3Util}.
 */
@RunWith(JUnit4.class)
public class S3UtilTest {

  @Test
  public void testGlobTranslation() {
    assertEquals("foo", S3Util.wildcardToRegexp("foo"));
    assertEquals("fo[^/]*o", S3Util.wildcardToRegexp("fo*o"));
    assertEquals("f[^/]*o\\.[^/]", S3Util.wildcardToRegexp("f*o.?"));
    assertEquals("foo-[0-9][^/]*", S3Util.wildcardToRegexp("foo-[0-9]*"));
    assertEquals("foo-[0-9].*", S3Util.wildcardToRegexp("foo-[0-9]**"));
    assertEquals(".*foo", S3Util.wildcardToRegexp("**/*foo"));
    assertEquals(".*foo", S3Util.wildcardToRegexp("**foo"));
    assertEquals("foo/[^/]*", S3Util.wildcardToRegexp("foo/*"));
    assertEquals("foo[^/]*", S3Util.wildcardToRegexp("foo*"));
    assertEquals("foo/[^/]*/[^/]*/[^/]*", S3Util.wildcardToRegexp("foo/*/*/*"));
    assertEquals("foo/[^/]*/.*", S3Util.wildcardToRegexp("foo/*/**"));
    assertEquals("foo.*baz", S3Util.wildcardToRegexp("foo**baz"));
  }

  private static S3Options s3Options() {
    S3Options options = PipelineOptionsFactory.as(S3Options.class);
    options.setAwsAccessKeyId("mock");
    options.setAwsSecretAccessKey("mock");
    options.setAwsRegion("us-west-1");
    return options;
  }

  @Test
  public void testCopyMultipleParts() throws IOException {
    S3Options pipelineOptions = s3Options();
    S3Util s3Util = pipelineOptions.getS3Util();

    AmazonS3 mockAmazonS3 = Mockito.mock(AmazonS3.class);
    s3Util.setAmazonS3Client(mockAmazonS3);

    S3Path sourcePath = S3Path.fromUri("s3://bucket/from");
    S3Path destinationPath = S3Path.fromUri("s3://bucket/to");

    InitiateMultipartUploadResult initiateMultipartUploadResult =
        new InitiateMultipartUploadResult();
    initiateMultipartUploadResult.setUploadId("upload-id");
    when(mockAmazonS3.initiateMultipartUpload(
        argThat(notNullValue(InitiateMultipartUploadRequest.class))))
        .thenReturn(initiateMultipartUploadResult);

    ObjectMetadata sourceS3ObjectMetadata = new ObjectMetadata();
    sourceS3ObjectMetadata.setContentLength((long) (s3Util.getS3UploadBufferSizeBytes() * 1.5));
    when(mockAmazonS3.getObjectMetadata(sourcePath.getBucket(), sourcePath.getKey()))
        .thenReturn(sourceS3ObjectMetadata);

    CopyPartResult copyPartResult1 = new CopyPartResult();
    copyPartResult1.setETag("etag-1");
    CopyPartResult copyPartResult2 = new CopyPartResult();
    copyPartResult1.setETag("etag-2");
    when(mockAmazonS3.copyPart(argThat(notNullValue(CopyPartRequest.class))))
        .thenReturn(copyPartResult1)
        .thenReturn(copyPartResult2);

    s3Util.copy(sourcePath, destinationPath);

    verify(mockAmazonS3, times(1))
        .completeMultipartUpload(argThat(notNullValue(CompleteMultipartUploadRequest.class)));
  }

  @Test
  public void deleteThousandsOfObjectsInMultipleBuckets() throws IOException {
    S3Options pipelineOptions = s3Options();
    S3Util s3Util = pipelineOptions.getS3Util();

    AmazonS3 mockAmazonS3 = Mockito.mock(AmazonS3.class);
    s3Util.setAmazonS3Client(mockAmazonS3);

    List<String> buckets = ImmutableList.of("bucket1", "bucket2");
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < 2500; i++) {
      keys.add(String.format("key-%d", i));
    }
    List<S3Path> paths = new ArrayList<>();
    for (String bucket : buckets) {
      for (String key : keys) {
        paths.add(S3Path.fromComponents(bucket, key));
      }
    }

    s3Util.delete(paths);

    // Should require 6 calls to delete 2500 objects in each of 2 buckets.
    verify(mockAmazonS3, times(6)).deleteObjects(argThat(notNullValue(DeleteObjectsRequest.class)));
  }

  @Test
  public void matchNonGlob() {
    S3Options pipelineOptions = s3Options();
    S3Util s3Util = pipelineOptions.getS3Util();

    AmazonS3 mockAmazonS3 = Mockito.mock(AmazonS3.class);
    s3Util.setAmazonS3Client(mockAmazonS3);

    S3Path path = S3Path.fromUri("s3://testbucket/testdirectory/filethatexists");
    ObjectMetadata s3ObjectMetadata = new ObjectMetadata();
    s3ObjectMetadata.setContentLength(100);
    when(mockAmazonS3.getObjectMetadata(path.getBucket(), path.getKey()))
        .thenReturn(s3ObjectMetadata);

    MatchResult result = s3Util.matchNonGlobPath(path);
    assertThat(
        result,
        MatchResultMatcher.create(
            ImmutableList.of(
                MatchResult.Metadata.builder()
                    .setSizeBytes(100)
                    .setResourceId(S3ResourceId.fromS3Path(path))
                    .setIsReadSeekEfficient(true)
                    .build())));
  }

  @Test
  public void matchNonGlobNotFound() throws IOException {
    S3Options pipelineOptions = s3Options();
    S3Util s3Util = pipelineOptions.getS3Util();

    AmazonS3 mockAmazonS3 = Mockito.mock(AmazonS3.class);
    s3Util.setAmazonS3Client(mockAmazonS3);

    S3Path path = S3Path.fromUri("s3://testbucket/testdirectory/nonexistentfile");
    AmazonS3Exception exception = new AmazonS3Exception("mock exception");
    exception.setStatusCode(404);
    when(mockAmazonS3.getObjectMetadata(path.getBucket(), path.getKey())).thenThrow(exception);

    MatchResult result = s3Util.matchNonGlobPath(path);
    assertThat(
        result,
        MatchResultMatcher.create(MatchResult.Status.NOT_FOUND, new FileNotFoundException()));
  }

  @Test
  public void matchNonGlobForbidden() throws IOException {
    S3Options pipelineOptions = s3Options();
    S3Util s3Util = pipelineOptions.getS3Util();

    AmazonS3 mockAmazonS3 = Mockito.mock(AmazonS3.class);
    s3Util.setAmazonS3Client(mockAmazonS3);

    AmazonS3Exception exception = new AmazonS3Exception("mock exception");
    exception.setStatusCode(403);
    S3Path path = S3Path.fromUri("s3://testbucket/testdirectory/keyname");
    when(mockAmazonS3.getObjectMetadata(path.getBucket(), path.getKey())).thenThrow(exception);

    assertThat(
        s3Util.matchNonGlobPath(path),
        MatchResultMatcher.create(MatchResult.Status.ERROR, new IOException(exception)));
  }

  static class ListObjectsV2RequestArgumentMatches extends ArgumentMatcher<ListObjectsV2Request> {

    private final ListObjectsV2Request expected;

    ListObjectsV2RequestArgumentMatches(ListObjectsV2Request expected) {
      this.expected = checkNotNull(expected);
    }

    @Override
    public boolean matches(Object argument) {
      if (argument != null && argument instanceof ListObjectsV2Request) {
        ListObjectsV2Request actual = (ListObjectsV2Request) argument;
        return expected.getBucketName().equals(actual.getBucketName())
            && expected.getPrefix().equals(actual.getPrefix())
            && (expected.getContinuationToken() == null
            ? actual.getContinuationToken() == null
            : expected.getContinuationToken().equals(actual.getContinuationToken()));
      }
      return false;
    }
  }

  @Test
  public void matchGlob() {
    S3Options pipelineOptions = s3Options();
    S3Util s3Util = pipelineOptions.getS3Util();

    AmazonS3 mockAmazonS3 = Mockito.mock(AmazonS3.class);
    s3Util.setAmazonS3Client(mockAmazonS3);

    S3Path path = S3Path.fromUri("s3://testbucket/foo/bar*baz");

    ListObjectsV2Request firstRequest =
        new ListObjectsV2Request()
            .withBucketName(path.getBucket())
            .withPrefix(path.getKeyNonWildcardPrefix())
            .withContinuationToken(null);

    // Expected to be returned; prefix and wildcard/regex match
    S3ObjectSummary firstMatch = new S3ObjectSummary();
    firstMatch.setBucketName(path.getBucket());
    firstMatch.setKey("foo/bar0baz");
    firstMatch.setSize(100);

    // Expected to not be returned; prefix matches, but substring after wildcard does not
    S3ObjectSummary secondMatch = new S3ObjectSummary();
    secondMatch.setBucketName(path.getBucket());
    secondMatch.setKey("foo/bar1qux");
    secondMatch.setSize(200);

    // Expected first request returns continuation token
    ListObjectsV2Result firstResult = new ListObjectsV2Result();
    firstResult.setNextContinuationToken("token");
    firstResult.getObjectSummaries().add(firstMatch);
    firstResult.getObjectSummaries().add(secondMatch);
    when(mockAmazonS3.listObjectsV2(argThat(new ListObjectsV2RequestArgumentMatches(firstRequest))))
        .thenReturn(firstResult);

    // Expect second request with continuation token
    ListObjectsV2Request secondRequest =
        new ListObjectsV2Request()
            .withBucketName(path.getBucket())
            .withPrefix(path.getKeyNonWildcardPrefix())
            .withContinuationToken("token");

    // Expected to be returned; prefix and wildcard/regex match
    S3ObjectSummary thirdMatch = new S3ObjectSummary();
    thirdMatch.setBucketName(path.getBucket());
    thirdMatch.setKey("foo/bar2baz");
    thirdMatch.setSize(300);

    // Expected second request returns third prefix match and no continuation token
    ListObjectsV2Result secondResult = new ListObjectsV2Result();
    secondResult.setNextContinuationToken(null);
    secondResult.getObjectSummaries().add(thirdMatch);
    when(mockAmazonS3.listObjectsV2(
        argThat(new ListObjectsV2RequestArgumentMatches(secondRequest))))
        .thenReturn(secondResult);

    assertThat(
        s3Util.matchGlobPath(path),
        MatchResultMatcher.create(
            ImmutableList.of(
                MatchResult.Metadata.builder()
                    .setIsReadSeekEfficient(true)
                    .setResourceId(
                        S3ResourceId.fromS3Path(
                            S3Path.fromComponents(firstMatch.getBucketName(), firstMatch.getKey())))
                    .setSizeBytes(firstMatch.getSize())
                    .build(),
                MatchResult.Metadata.builder()
                    .setIsReadSeekEfficient(true)
                    .setResourceId(
                        S3ResourceId.fromS3Path(
                            S3Path.fromComponents(thirdMatch.getBucketName(), thirdMatch.getKey())))
                    .setSizeBytes(thirdMatch.getSize())
                    .build())));
  }

  @Test
  public void matchVariousInvokeThreadPool() throws IOException {
    S3Options pipelineOptions = s3Options();
    S3Util s3Util = pipelineOptions.getS3Util();

    AmazonS3 mockAmazonS3 = Mockito.mock(AmazonS3.class);
    s3Util.setAmazonS3Client(mockAmazonS3);

    AmazonS3Exception notFoundException = new AmazonS3Exception("mock exception");
    notFoundException.setStatusCode(404);
    S3Path pathNotExist = S3Path.fromUri("s3://testbucket/testdirectory/nonexistentfile");
    when(mockAmazonS3.getObjectMetadata(pathNotExist.getBucket(), pathNotExist.getKey()))
        .thenThrow(notFoundException);

    AmazonS3Exception forbiddenException = new AmazonS3Exception("mock exception");
    forbiddenException.setStatusCode(403);
    S3Path pathForbidden = S3Path.fromUri("s3://testbucket/testdirectory/forbiddenfile");
    when(mockAmazonS3.getObjectMetadata(pathForbidden.getBucket(), pathForbidden.getKey()))
        .thenThrow(forbiddenException);

    S3Path pathExist = S3Path.fromUri("s3://testbucket/testdirectory/filethatexists");
    ObjectMetadata s3ObjectMetadata = new ObjectMetadata();
    s3ObjectMetadata.setContentLength(100);
    when(mockAmazonS3.getObjectMetadata(pathExist.getBucket(), pathExist.getKey()))
        .thenReturn(s3ObjectMetadata);

    S3Path pathGlob = S3Path.fromUri("s3://testbucket/path/part*");

    S3ObjectSummary foundListObject = new S3ObjectSummary();
    foundListObject.setBucketName(pathGlob.getBucket());
    foundListObject.setKey("path/part-0");
    foundListObject.setSize(200);

    ListObjectsV2Result listObjectsResult = new ListObjectsV2Result();
    listObjectsResult.setNextContinuationToken(null);
    listObjectsResult.getObjectSummaries().add(foundListObject);
    when(mockAmazonS3.listObjectsV2(notNull(ListObjectsV2Request.class)))
        .thenReturn(listObjectsResult);

    assertThat(
        s3Util.match(ImmutableList.of(pathNotExist, pathForbidden, pathExist, pathGlob)),
        contains(
            MatchResultMatcher.create(MatchResult.Status.NOT_FOUND, new FileNotFoundException()),
            MatchResultMatcher.create(
                MatchResult.Status.ERROR, new IOException(forbiddenException)),
            MatchResultMatcher.create(100, S3ResourceId.fromS3Path(pathExist), true),
            MatchResultMatcher.create(
                200,
                S3ResourceId.fromS3Path(
                    S3Path.fromComponents(pathGlob.getBucket(), foundListObject.getKey())),
                true)));
  }
}
