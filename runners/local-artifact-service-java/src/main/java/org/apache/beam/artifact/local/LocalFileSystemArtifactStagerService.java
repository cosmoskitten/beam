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

import com.google.common.base.Throwables;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.Artifact;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.CommitManifestRequest;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.CommitManifestResponse;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.PutArtifactRequest;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.PutArtifactRequest.ContentCase;
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
    if ((stagingBase.mkdirs() || stagingBase.exists()) && stagingBase.canWrite()) {
      artifactsBase = new File(stagingBase, "artifacts");
      checkState(
          (artifactsBase.mkdir() || artifactsBase.exists()) && artifactsBase.canWrite(),
          "Could not create artifact staging directory at %s",
          artifactsBase);
    } else {
      throw new IllegalStateException(
          String.format("Could not create staging directory structure at root %s", stagingBase));
    }
  }

  @Override
  public StreamObserver<PutArtifactRequest> putArtifact(
      final StreamObserver<PutArtifactResponse> responseObserver) {
    return new CreateAndWriteFileObserver(responseObserver);
  }

  @Override
  public void commitManifest(
      CommitManifestRequest request, StreamObserver<CommitManifestResponse> responseObserver) {
    try {
      Collection<Artifact> missing = new ArrayList<>();
      for (Artifact artifact : request.getManifest().getArtifactList()) {
        // TODO: Validate the checksums on the server side, to fail more aggressively if require
        if (!getArtifactFile(artifact.getName()).exists()) {
          missing.add(artifact);
        }
      }
      if (!missing.isEmpty()) {
        throw Status.INVALID_ARGUMENT
            .withDescription(
                String.format("Attempted to commit manifest with missing Artifacts: [%s]", missing))
            .asRuntimeException();
      }
      File mf = new File(stagingBase, "MANIFEST");
      checkState(mf.createNewFile(), "Could not create file to store manifest");
      try (OutputStream mfOut = new FileOutputStream(mf)) {
        request.getManifest().writeTo(mfOut);
      }
      responseObserver.onNext(
          CommitManifestResponse.newBuilder()
              .setStagingToken(stagingBase.getCanonicalPath())
              .build());
      responseObserver.onCompleted();
    } catch (StatusRuntimeException e) {
      responseObserver.onError(e);
      LOG.error("Failed to commit Manifest {}", request.getManifest(), e);
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL
              .withCause(e)
              .withDescription(Throwables.getStackTraceAsString(e))
              .asRuntimeException());
      LOG.error("Failed to commit Manifest {}", request.getManifest(), e);
    }
  }

  File getArtifactFile(String artifactName) {
    return new File(artifactsBase, artifactName);
  }

  private class CreateAndWriteFileObserver implements StreamObserver<PutArtifactRequest> {
    private final StreamObserver<PutArtifactResponse> responseObserver;
    private FileWritingObserver writer;

    private CreateAndWriteFileObserver(StreamObserver<PutArtifactResponse> responseObserver) {
      this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(PutArtifactRequest value) {
      try {
        if (writer == null) {
          if (!value.getContentCase().equals(ContentCase.NAME)) {
            throw Status.INVALID_ARGUMENT
                .withDescription(
                    String.format(
                        "Expected the first %s to contain the Artifact Name, got %s",
                        PutArtifactRequest.class.getSimpleName(), value.getContentCase()))
                .asRuntimeException();
          }
          writer = createFile(value.getName());
        } else {
          writer.onNext(value);
        }
      } catch (StatusRuntimeException e) {
        responseObserver.onError(e);
      } catch (Exception e) {
        responseObserver.onError(
            Status.INTERNAL
                .withCause(e)
                .withDescription(Throwables.getStackTraceAsString(e))
                .asRuntimeException());
      }
    }

    private FileWritingObserver createFile(String artifactName) throws IOException {
      File destination = getArtifactFile(artifactName);
      if (!destination.createNewFile()) {
        throw Status.ALREADY_EXISTS
            .withDescription(String.format("Artifact with name %s already exists", artifactName))
            .asRuntimeException();
      }
      return new FileWritingObserver(
          destination, new FileOutputStream(destination), responseObserver);
    }

    @Override
    public void onError(Throwable t) {
      if (writer != null) {
        writer.onError(t);
      } else {
        responseObserver.onCompleted();
      }
    }

    @Override
    public void onCompleted() {
      if (writer != null) {
        writer.onCompleted();
      } else {
        responseObserver.onCompleted();
      }
    }
  }

  private static class FileWritingObserver implements StreamObserver<PutArtifactRequest> {
    private final File destination;
    private final OutputStream target;
    private final StreamObserver<PutArtifactResponse> responseObserver;

    private FileWritingObserver(
        File destination,
        OutputStream target,
        StreamObserver<PutArtifactResponse> responseObserver) {
      this.destination = destination;
      this.target = target;
      this.responseObserver = responseObserver;
    }

    @Override
    public void onNext(PutArtifactRequest value) {
      try {
        if (value.getData() == null) {
          StatusRuntimeException e = Status.INVALID_ARGUMENT.withDescription(String.format(
              "Expected all chunks in the current stream state to contain data, got %s",
              value.getContentCase())).asRuntimeException();
          throw e;
        }
        value.getData().getData().writeTo(target);
      } catch (Exception e) {
        cleanedUp(e);
      }
    }

    @Override
    public void onError(Throwable t) {
      if (cleanedUp(null)) {
        responseObserver.onCompleted();
      }
    }

    @Override
    public void onCompleted() {
      try {
        target.close();
      } catch (IOException e) {
        LOG.error("Failed to complete writing file {}", destination, e);
        cleanedUp(e);
        return;
      }
      responseObserver.onNext(PutArtifactResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }

    /**
     * Cleans up after the file writing failed exceptionally, due to an error either in the service
     * or sent from the client.
     *
     * @return false if an error was reported, true otherwise
     */
    private boolean cleanedUp(@Nullable Throwable whyFailed) {
      Throwable actual = whyFailed;
      try {
        target.close();
        if (!destination.delete()) {
          LOG.debug("Couldn't delete failed write at {}", destination);
        }
      } catch (IOException e) {
        if (whyFailed == null) {
          actual = e;
        } else {
          actual.addSuppressed(e);
        }
        LOG.error("Failed to clean up after writing file {}", destination, e);
      }
      if (actual != null) {
        if (actual instanceof StatusException || actual instanceof StatusRuntimeException) {
          responseObserver.onError(actual);
        } else {
          Status status =
              Status.INTERNAL
                  .withCause(actual)
                  .withDescription(Throwables.getStackTraceAsString(actual));
          responseObserver.onError(status.asException());
        }
      }
      return actual == null;
    }
  }
}
