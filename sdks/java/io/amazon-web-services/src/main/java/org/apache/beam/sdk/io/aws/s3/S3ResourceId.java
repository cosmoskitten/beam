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
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;

/** {@link ResourceId} implementation for Amazon Web Services S3. */
public class S3ResourceId implements ResourceId {

  private final S3Path s3Path;

  public static S3ResourceId fromS3Path(S3Path s3Path) {
    checkNotNull(s3Path, "s3Path");
    return new S3ResourceId(s3Path);
  }

  private S3ResourceId(S3Path s3Path) {
    this.s3Path = s3Path;
  }

  public S3Path getS3Path() {
    return s3Path;
  }

  static List<S3ResourceId> getNonDirectoryResourceIds(Collection<S3ResourceId> resourceIds) {
    return FluentIterable.from(resourceIds)
        .filter(
            new Predicate<S3ResourceId>() {
              @Override
              public boolean apply(S3ResourceId resourceId) {
                return !resourceId.isDirectory();
              }
            })
        .toList();
  }

  static List<S3Path> getS3Paths(Collection<S3ResourceId> resourceIds) {
    return FluentIterable.from(resourceIds)
        .transform(
            new Function<S3ResourceId, S3Path>() {
              @Override
              public S3Path apply(S3ResourceId resourceId) {
                return resourceId.getS3Path();
              }
            })
        .toList();
  }

  @Override
  public ResourceId resolve(String other, ResolveOptions resolveOptions) {
    checkState(
        isDirectory(), String.format("Expected the s3Path is a directory, but had [%s].", s3Path));
    checkArgument(
        resolveOptions.equals(StandardResolveOptions.RESOLVE_FILE)
            || resolveOptions.equals(StandardResolveOptions.RESOLVE_DIRECTORY),
        String.format("ResolveOptions: [%s] is not supported.", resolveOptions));
    if (resolveOptions.equals(StandardResolveOptions.RESOLVE_FILE)) {
      checkArgument(
          !other.endsWith("/"), "The resolved file: [%s] should not end with '/'.", other);
      return fromS3Path(s3Path.resolve(other));
    }

    // StandardResolveOptions.RESOLVE_DIRECTORY
    if (other.endsWith("/")) {
      // other already contains the delimiter for S3.
      // It is not recommended for callers to set the delimiter.
      // However, we consider it as a valid input.
      return fromS3Path(s3Path.resolve(other));
    }
    return fromS3Path(s3Path.resolve(other + "/"));
  }

  @Override
  public ResourceId getCurrentDirectory() {
    if (isDirectory()) {
      return this;
    }
    S3Path parent = s3Path.getParent();
    checkState(
        parent != null,
        String.format("Failed to get the current directory for path: [%s].", s3Path));
    return fromS3Path(parent);
  }

  @Nullable
  @Override
  public String getFilename() {
    if (s3Path.getNameCount() <= 1) {
      return null;
    }
    S3Path s3Filename = s3Path.getFileName();
    return s3Filename == null ? null : s3Filename.toString();
  }

  @Override
  public boolean isDirectory() {
    return s3Path.endsWith("/");
  }

  @Override
  public String getScheme() {
    return S3Path.SCHEME;
  }

  @Override
  public String toString() {
    return s3Path.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof S3ResourceId)) {
      return false;
    }
    S3ResourceId otherS3ResourceId = (S3ResourceId) other;
    return s3Path.equals(otherS3ResourceId.s3Path);
  }

  @Override
  public int hashCode() {
    return s3Path.hashCode();
  }
}
