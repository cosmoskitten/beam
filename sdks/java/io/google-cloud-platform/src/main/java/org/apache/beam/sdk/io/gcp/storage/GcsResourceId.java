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

import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.gcsfs.GcsPath;

/**
 * {@link ResourceId} implementation for Google Cloud Storage.
 */
public class GcsResourceId implements ResourceId {

  private final GcsPath gcsPath;

  static GcsResourceId fromGcsPath(GcsPath gcsPath) {
    return new GcsResourceId(gcsPath);
  }

  private GcsResourceId(GcsPath gcsPath) {
    this.gcsPath = gcsPath;
  }

  @Override
  public ResourceId resolve(String other) {
    checkState(
        isDirectory(),
        String.format("Expected the gcsPath is a directory, but had [%s].", gcsPath));
    return fromGcsPath(gcsPath.resolve(other));
  }

  @Override
  public ResourceId getCurrentDirectory() {
    if (isDirectory()) {
      return this;
    } else {
      return fromGcsPath(gcsPath.getParent());
    }
  }

  private boolean isDirectory() {
    return gcsPath.endsWith("/");
  }

  @Override
  public String getScheme() {
    return GcsFileSystemRegistrar.GCS_SCHEME;
  }

  GcsPath getGcsPath() {
    return gcsPath;
  }

  @Override
  public String toString() {
    return String.format("GcsResourceId: [%s]", gcsPath);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof GcsResourceId)) {
      return false;
    }
    GcsResourceId other = (GcsResourceId) obj;
    return this.gcsPath.equals(other.gcsPath);
  }

  @Override
  public int hashCode() {
    return gcsPath.hashCode();
  }
}
