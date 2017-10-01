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
package org.apache.beam.sdk.io.aws.s3;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;

class S3FileSystem extends FileSystem<S3ResourceId> {

  private final S3Options options;

  S3FileSystem(S3Options options) {
    this.options = checkNotNull(options, "options");
  }

  @Override
  protected String getScheme() {
    return S3Path.SCHEME;
  }

  @Override
  protected List<MatchResult> match(List<String> specs) throws IOException {
    List<S3Path> paths =
        FluentIterable.from(specs)
            .transform(
                new Function<String, S3Path>() {
                  @Override
                  public S3Path apply(String spec) {
                    return S3Path.fromUri(spec);
                  }
                })
            .toList();
    return options.getS3Util().match(paths);
  }

  @Override
  protected WritableByteChannel create(S3ResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    return options.getS3Util().create(resourceId.getS3Path());
  }

  @Override
  protected ReadableByteChannel open(S3ResourceId resourceId) throws IOException {
    return options.getS3Util().open(resourceId.getS3Path());
  }

  @Override
  protected void copy(
      List<S3ResourceId> sourceResourceIds, List<S3ResourceId> destinationResourceIds)
      throws IOException {
    List<S3Path> sourcePaths = S3ResourceId.getS3Paths(sourceResourceIds);
    List<S3Path> destinationPaths = S3ResourceId.getS3Paths(destinationResourceIds);
    options.getS3Util().copy(sourcePaths, destinationPaths);
  }

  @Override
  protected void rename(
      List<S3ResourceId> sourceResourceIds, List<S3ResourceId> destinationResourceIds)
      throws IOException {
    copy(sourceResourceIds, destinationResourceIds);
    delete(sourceResourceIds);
  }

  @Override
  protected void delete(Collection<S3ResourceId> resourceIds) throws IOException {
    List<S3ResourceId> nonDirectoryResourceIds =
        S3ResourceId.getNonDirectoryResourceIds(resourceIds);
    List<S3Path> paths = S3ResourceId.getS3Paths(nonDirectoryResourceIds);
    options.getS3Util().delete(paths);
  }

  @Override
  protected S3ResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
    if (isDirectory) {
      if (!singleResourceSpec.endsWith("/")) {
        singleResourceSpec += "/";
      }
    } else {
      checkArgument(
          !singleResourceSpec.endsWith("/"),
          "Expected a file path, but [%s] ends with '/'. This is unsupported in S3FileSystem.",
          singleResourceSpec);
    }
    S3Path path = S3Path.fromUri(singleResourceSpec);
    return S3ResourceId.fromS3Path(path);
  }
}
