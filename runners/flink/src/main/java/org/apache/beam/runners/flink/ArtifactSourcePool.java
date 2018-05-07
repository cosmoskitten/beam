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
package org.apache.beam.runners.flink;

import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.Serializable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;
import org.apache.beam.runners.fnexecution.artifact.ArtifactSource;
import org.apache.flink.api.common.cache.DistributedCache;

/**
 * A pool of {@link DistributedCache DistributedCaches} to use as artifact sources. A cache must be
 * registered before artifacts are requested from it.
 */
@ThreadSafe
public class ArtifactSourcePool implements ArtifactSource {

  /**
   * Factory for creating {@link ArtifactSourcePool cache pools}. Must be serializable for
   * distribution to TaskManagers.
   */
  public interface Factory extends Serializable {
    /** Gets or creates a cache pool for the given job id. */
    ArtifactSourcePool forJob(String jobId);
  }

  /** Retrieves the default distributed cache pool factory. */
  public static Factory defaultFactory() {
    return (jobId) -> {
      throw new UnsupportedOperationException();
    };
  }

  private ArtifactSourcePool() {}

  /**
   * Adds a new cache to the pool. When the returned {@link AutoCloseable} is closed, the given
   * cache will be removed from the pool.
   */
  public AutoCloseable addCacheToPool(DistributedCache distributedCache) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Manifest getManifest() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getArtifact(String name, StreamObserver<ArtifactChunk> responseObserver) {
    throw new UnsupportedOperationException();
  }
}
