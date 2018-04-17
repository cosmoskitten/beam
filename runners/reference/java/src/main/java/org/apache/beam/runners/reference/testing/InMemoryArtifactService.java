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
package org.apache.beam.runners.reference.testing;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.CommitManifestResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.PutArtifactResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc.ArtifactStagingServiceImplBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A StagingService for tests. */
public class InMemoryArtifactService extends ArtifactStagingServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryArtifactService.class);

  private final Object artifactLock = new Object();

  @GuardedBy("artifactLock")
  private final Map<ArtifactMetadata, ByteString> artifacts = Maps.newHashMap();

  private final boolean keepArtifacts;

  @GuardedBy("artifactLock")
  private boolean committed = false;

  public InMemoryArtifactService(boolean keepArtifacts) {
    this.keepArtifacts = keepArtifacts;
  }

  @Override
  public StreamObserver<PutArtifactRequest> putArtifact(
      StreamObserver<PutArtifactResponse> responseObserver) {
    return new PutArtifactRequestObserver(responseObserver, keepArtifacts, this::putArtifact);
  }

  @Override
  public void commitManifest(
      CommitManifestRequest request, StreamObserver<CommitManifestResponse> responseObserver) {
    LOG.debug("Committing manifest: {}", request);
    try {
      Set<ArtifactMetadata> commitMetadata =
          Sets.newHashSet(request.getManifest().getArtifactList());
      synchronized (artifactLock) {
        checkState(!committed, "Manifest already committed");
        // TODO: Consider comparing only by artifact name if checksums are not sent.
        Set<ArtifactMetadata> missingFromServer =
            Sets.difference(commitMetadata, artifacts.keySet());
        checkArgument(
            missingFromServer.isEmpty(),
            "Artifacts from request never uploaded: %s",
            missingFromServer);

        Set<ArtifactMetadata> extraOnServer = Sets.difference(artifacts.keySet(), commitMetadata);
        checkArgument(extraOnServer.isEmpty(), "Extraneous artifacts received: %s", extraOnServer);
        // NOTE: We do not attempt to commit transactionally; if a commit fails, the artifact server
        // is assumed to be in a bad state.
        committed = true;
        responseObserver.onNext(CommitManifestResponse.getDefaultInstance());
        responseObserver.onCompleted();
      }
    } catch (Exception e) {
      LOG.warn("Error committing manifest", e);
      responseObserver.onError(
          Status.INVALID_ARGUMENT
              .withDescription(e.getMessage())
              .withCause(e)
              .asRuntimeException());
    }
  }

  private void putArtifact(ArtifactMetadata metadata, ByteString artifactBytes) {
    synchronized (artifactLock) {
      checkState(!committed, "Manifest already committed");
      LOG.debug("Adding artifact: {}", metadata);
      artifacts.put(metadata, artifactBytes);
    }
  }

  private static class PutArtifactRequestObserver implements StreamObserver<PutArtifactRequest> {

    private final StreamObserver<PutArtifactResponse> responseObserver;
    private final boolean keepArtifacts;
    private final BiConsumer<ArtifactMetadata, ByteString> commitConsumer;

    private ArtifactMetadata metadata;
    private ByteString artifactBytes;
    private MessageDigest md5Digest;

    PutArtifactRequestObserver(
        StreamObserver<PutArtifactResponse> responseObserver,
        boolean keepArtifacts,
        BiConsumer<ArtifactMetadata, ByteString> commitConsumer) {
      this.responseObserver = responseObserver;
      this.keepArtifacts = keepArtifacts;
      this.commitConsumer = commitConsumer;
    }

    @Override
    public void onNext(PutArtifactRequest request) {
      try {
        if (this.metadata == null) {
          ArtifactMetadata metadata = request.getMetadata();
          String name = metadata.getName();
          checkArgument(name != null && !name.isEmpty(), "Artifact name required");
          LOG.debug("Starting artifact upload for: {}", metadata);
          this.metadata = metadata;
          this.artifactBytes = ByteString.EMPTY;
          this.md5Digest = MessageDigest.getInstance("MD5");
        } else {
          checkArgument(artifactBytes != null, "Artifact metadata must be supplied before data");
          md5Digest.update(request.getData().getData().asReadOnlyByteBuffer());
          if (keepArtifacts) {
            artifactBytes = artifactBytes.concat(request.getData().getData());
          }
        }
      } catch (Exception e) {
        LOG.warn("Error uploading artifact", e);
        responseObserver.onError(
            Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
      }
    }

    @Override
    public void onError(Throwable throwable) {
      responseObserver.onError(throwable);
    }

    @Override
    public void onCompleted() {
      try {
        checkState(metadata != null, "Missing artifact metadata");
        checkState(artifactBytes != null, "Missing artifact bytes");
        String md5String = BaseEncoding.base64().encode(md5Digest.digest());
        commitConsumer.accept(metadata.toBuilder().setMd5(md5String).build(), artifactBytes);
        responseObserver.onNext(PutArtifactResponse.getDefaultInstance());
        responseObserver.onCompleted();
      } catch (Exception e) {
        LOG.warn("Error adding artifact", e);
        responseObserver.onError(
            Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
      }
    }
  }
}
