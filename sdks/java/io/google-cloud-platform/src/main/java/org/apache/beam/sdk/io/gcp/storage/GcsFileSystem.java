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
package org.apache.beam.sdk.io.gcp.storage;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.FileSystems.CreateOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.util.gcsfs.GcsPath;

/**
 * {@link FileSystem} implementation for Google Cloud Storage.
 */
public class GcsFileSystem extends FileSystem<GcsResourceId> {
  private final GcsOptions options;

  GcsFileSystem(GcsOptions options) {
    this.options = checkNotNull(options, "options");
  }

  @Override
  protected WritableByteChannel create(GcsResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    if (createOptions instanceof GcsCreateOptions) {
      return options.getGcsUtil().create(
          resourceId.getGcsPath(),
          createOptions.mimeType(),
          ((GcsCreateOptions) createOptions).uploadBufferSizeBytes());
    } else {
      return options.getGcsUtil().create(resourceId.getGcsPath(), createOptions.mimeType());
    }
  }

  @Override
  protected ReadableByteChannel open(GcsResourceId resourceId) throws IOException {
    return options.getGcsUtil().open(resourceId.getGcsPath());
  }

  @Override
  protected void rename(
      List<GcsResourceId> srcResourceIds,
      List<GcsResourceId> destResourceIds) throws IOException {
    copy(srcResourceIds, destResourceIds);
    delete(srcResourceIds);
  }

  @Override
  protected void delete(Collection<GcsResourceId> resourceIds) throws IOException {
    options.getGcsUtil().remove(toFilenames(resourceIds));
  }

  @Override
  protected void copy(List<GcsResourceId> srcResourceIds, List<GcsResourceId> destResourceIds)
      throws IOException {
    options.getGcsUtil().copy(toFilenames(srcResourceIds), toFilenames(destResourceIds));
  }

  /**
   * A configuration options for {@link GcsFileSystem}.
   */
  @AutoValue
  public abstract static class GcsCreateOptions extends CreateOptions {
    public abstract int uploadBufferSizeBytes();

    /**
     * Returns a {@link GcsCreateOptions.Builder}.
     */
    public static GcsCreateOptions.Builder builder() {
      return new AutoValue_GcsFileSystem_GcsCreateOptions.Builder();
    }

    /**
     * Builder for {@link GcsCreateOptions}.
     */
    @AutoValue.Builder
    public abstract static class Builder extends CreateOptions.Builder<Builder> {
      public abstract Builder setUploadBufferSizeBytes(int value);

      public abstract GcsCreateOptions build();
    }
  }

  private List<String> toFilenames(Collection<GcsResourceId> resources) {
    return FluentIterable.from(resources)
        .transform(
            new Function<GcsResourceId, String>() {
              @Override
              public String apply(GcsResourceId resource) {
                return resource.getGcsPath().toString();
              }})
        .toList();
  }
}
