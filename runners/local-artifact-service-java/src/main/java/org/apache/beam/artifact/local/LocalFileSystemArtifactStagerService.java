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

package org.apache.beam.artifact.local;

import static com.google.common.base.Preconditions.checkState;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.Artifact;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.CommitManifestRequest;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.CommitManifestResponse;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.PutArtifactRequest;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.PutArtifactResponse;
import org.apache.beam.sdk.common.runner.v1.ArtifactStagingServiceGrpc.ArtifactStagingServiceImplBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An {@code ArtifactStagingService} which stages files to a local temp directory. */
public class LocalFileSystemArtifactStagerService extends ArtifactStagingServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(LocalFileSystemArtifactStagerService.class);

  public static LocalFileSystemArtifactStagerService withRootDirectory(File base) {
    return new LocalFileSystemArtifactStagerService(base);
  }

  private final File stagingBase;
  private final File artifactsBase;

  private LocalFileSystemArtifactStagerService(File stagingBase) {
    this.stagingBase = stagingBase;
    stagingBase.mkdirs();
    if (stagingBase.exists() && stagingBase.canWrite()) {
      artifactsBase = new File(stagingBase, "artifacts");
    } else {
      throw new IllegalStateException(
          String.format("Cannot write to base directory %s", stagingBase));
    }
    checkState(
        stagingBase.exists() && stagingBase.canWrite(),
        "Could not create staging directory structure at root %s",
        stagingBase);
  }

  @Override
  public StreamObserver<PutArtifactRequest> putArtifact(
      final StreamObserver<PutArtifactResponse> responseObserver) {
    return new StreamObserver<PutArtifactRequest>() {
      private File destination;
      private FileChannel target;

      @Override
      public void onNext(PutArtifactRequest value) {
        switch (value.getContentCase()) {
          case NAME:
            if (destination != null) {
              StatusRuntimeException failure =
                  Status.INVALID_ARGUMENT
                      .withDescription(
                          String.format(
                              "Expected NAME only once as the first %s",
                              PutArtifactRequest.class.getSimpleName()))
                      .asRuntimeException();
              responseObserver.onError(failure);
              throw failure;
            }
            createFile(value.getName());
            return;
          case DATA:
            if (destination == null) {
              StatusRuntimeException failure =
                  Status.INVALID_ARGUMENT
                      .withDescription("Expected DATA only after the NAME has been provided")
                      .asRuntimeException();
              responseObserver.onError(failure);
              throw failure;
            }
            writeData(value.getData().getData().asReadOnlyByteBuffer());
            return;
          case CONTENT_NOT_SET:
          default:
            StatusRuntimeException failure =
                Status.INVALID_ARGUMENT
                    .withDescription("One of NAME or DATA is required")
                    .asRuntimeException();
            responseObserver.onError(failure);
            throw failure;
        }
      }

      private void createFile(String artifactName) {
        destination = getArtifactFile(artifactName);
        try {
          if (!destination.createNewFile()) {
            String msg = String.format("Artifact with name %s already exists", artifactName);
            StatusRuntimeException failure =
                Status.ALREADY_EXISTS.withDescription(msg).asRuntimeException();
            responseObserver.onError(failure);
            throw failure;
          }
          target = new FileOutputStream(destination).getChannel();
        } catch (IOException e) {
          StatusRuntimeException status = Status.INTERNAL.withCause(e).asRuntimeException();
          responseObserver.onError(status);
          LOG.error(
              "Failed to withRootDirectory file {} for artifact {} at base location {}",
              destination,
              artifactName,
              stagingBase,
              e);
          throw status;
        }
      }

      /**
       * Write the contents of the provided {@link ByteBuffer} to the target {@link File}.
       *
       * <p>Will read starting at the current position of the {@link ByteBuffer} and consume all of
       * the available bytes.
       */
      private void writeData(ByteBuffer byteBuffer) {
        try {
          target.write(byteBuffer);
        } catch (IOException e) {
          StatusRuntimeException failure = Status.INTERNAL.withCause(e).asRuntimeException();
          responseObserver.onError(failure);
          LOG.error("Failed to write contents to file {}", destination, e);
          throw failure;
        }
      }

      @Override
      public void onError(Throwable t) {
        try {
          target.close();
          // try to cleanup; no guarantees that this will work.
          destination.delete();
        } catch (IOException e) {
          t.addSuppressed(e);
        }
      }

      @Override
      public void onCompleted() {
        try {
          target.close();
        } catch (IOException e) {
          StatusRuntimeException failure = Status.INTERNAL.withCause(e).asRuntimeException();
          responseObserver.onError(failure);
          LOG.error("Failed to complete writing file {}", destination, e);
          throw failure;
        }
        responseObserver.onNext(PutArtifactResponse.getDefaultInstance());
        responseObserver.onCompleted();
      }
    };
  }

  @Override
  public void commitManifest(
      CommitManifestRequest request, StreamObserver<CommitManifestResponse> responseObserver) {
    Collection<Artifact> missing = new ArrayList<>();
    for (Artifact artifact : request.getManifest().getArtifactList()) {
      if (!getArtifactFile(artifact.getName()).exists()) {
        missing.add(artifact);
      }
    }
    if (missing.isEmpty()) {
      File mf = new File(stagingBase, "MANIFEST");
      try {
        checkState(mf.createNewFile());
        try (OutputStream mfOut = new FileOutputStream(mf)) {
          request.getManifest().writeTo(mfOut);
        }
        responseObserver.onNext(
            CommitManifestResponse.newBuilder()
                .setStagingToken(stagingBase.getCanonicalPath())
                .build());
        responseObserver.onCompleted();
      } catch (IOException e) {
        StatusRuntimeException failure = Status.INTERNAL.withCause(e).asRuntimeException();
        responseObserver.onError(failure);
        LOG.error("Failed to commit Manifest {}", request.getManifest(), e);
        throw failure;
      }
    } else {
      StatusRuntimeException failure =
          Status.INVALID_ARGUMENT
              .withDescription(
                  String.format(
                      "Attempted to commit manifest with missing Artifacts: [%s]", missing))
              .asRuntimeException();
      responseObserver.onError(failure);
      throw failure;
    }
  }

  private File getArtifactFile(String artifactName) {
    return new File(artifactsBase, artifactName);
  }
}
