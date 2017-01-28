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
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.FileSystems.CreateOptions;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.util.gcsfs.GcsPath;

/**
 * {@link FileSystem} implementation for Google Cloud Storage.
 */
public class GcsFileSystem extends FileSystem {
  private final GcsOptions options;

  GcsFileSystem(GcsOptions options) {
    this.options = checkNotNull(options, "options");
  }

  @Override
  protected WritableByteChannel create(String file, CreateOptions createOptions)
      throws IOException {
    if (createOptions instanceof GcsCreateOptions) {
      return options.getGcsUtil().create(
          GcsPath.fromUri(file),
          createOptions.mimeType(),
          ((GcsCreateOptions) createOptions).uploadBufferSizeBytes());
    } else {
      return options.getGcsUtil().create(
          GcsPath.fromUri(file),
          createOptions.mimeType());
    }
  }

  @Override
  protected ReadableByteChannel open(String file) throws IOException {
    return options.getGcsUtil().open(GcsPath.fromUri(file));
  }

  @Override
  protected void rename(List<String> srcFiles, List<String> destFiles) throws IOException {
    copy(srcFiles, destFiles);
    delete(srcFiles);
  }

  @Override
  protected void delete(Collection<String> files) throws IOException {
    options.getGcsUtil().remove(files);
  }

  @Override
  protected void copy(List<String> srcUris, List<String> destUris) throws IOException {
    options.getGcsUtil().copy(srcUris, destUris);
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
}
