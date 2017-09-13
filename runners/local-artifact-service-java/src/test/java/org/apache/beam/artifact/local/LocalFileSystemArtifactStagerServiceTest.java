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

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.Artifact;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.CommitManifestRequest;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.CommitManifestResponse;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.Manifest;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.PutArtifactRequest;
import org.apache.beam.sdk.common.runner.v1.ArtifactApi.PutArtifactResponse;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link LocalFileSystemArtifactStagerService}. */
@RunWith(JUnit4.class)
public class LocalFileSystemArtifactStagerServiceTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private LocalFileSystemArtifactStagerService stager;
  private File stagingLocation;

  @Before
  public void setup() throws Exception {
    stagingLocation = temporaryFolder.newFolder();
    stager = LocalFileSystemArtifactStagerService.create(stagingLocation);
  }

  @Test
  public void singleDataPutArtifactSucceeds() throws Exception {
    byte[] data = "foo-bar-baz".getBytes();
    RecordingStreamObserver<PutArtifactResponse> responseObserver = new RecordingStreamObserver<>();
    StreamObserver<PutArtifactRequest> requestObserver = stager.putArtifact(responseObserver);

    String name = "my-artifact";
    requestObserver.onNext(PutArtifactRequest.newBuilder().setName(name).build());
    requestObserver.onNext(
        PutArtifactRequest.newBuilder()
            .setData(ArtifactChunk.newBuilder().setData(ByteString.copyFrom(data)).build())
            .build());
    requestObserver.onCompleted();

    String[] recordedFiles = stagingLocation.list();
    Assert.assertThat(recordedFiles, Matchers.arrayWithSize(1));
    File staged = stagingLocation.toPath().resolve(recordedFiles[0]).toFile();
    ByteBuffer buf = ByteBuffer.allocate(data.length);
    new FileInputStream(staged).getChannel().read(buf);
    Assert.assertArrayEquals(data, buf.array());
  }

  @Test
  public void multiPartPutArtifactSucceeds() throws Exception {
    byte[] partOne = "foo-".getBytes();
    byte[] partTwo = "bar-".getBytes();
    byte[] partThree = "baz".getBytes();
    RecordingStreamObserver<PutArtifactResponse> responseObserver = new RecordingStreamObserver<>();
    StreamObserver<PutArtifactRequest> requestObserver = stager.putArtifact(responseObserver);

    String name = "my-artifact";
    requestObserver.onNext(PutArtifactRequest.newBuilder().setName(name).build());
    requestObserver.onNext(
        PutArtifactRequest.newBuilder()
            .setData(ArtifactChunk.newBuilder().setData(ByteString.copyFrom(partOne)).build())
            .build());
    requestObserver.onNext(
        PutArtifactRequest.newBuilder()
            .setData(ArtifactChunk.newBuilder().setData(ByteString.copyFrom(partTwo)).build())
            .build());
    requestObserver.onNext(
        PutArtifactRequest.newBuilder()
            .setData(ArtifactChunk.newBuilder().setData(ByteString.copyFrom(partThree)).build())
            .build());
    requestObserver.onCompleted();

    String[] recordedFiles = stagingLocation.list();
    Assert.assertThat(recordedFiles, Matchers.arrayWithSize(1));
    File staged = stagingLocation.toPath().resolve(recordedFiles[0]).toFile();
    ByteBuffer buf = ByteBuffer.allocate("foo-bar-baz".length());
    new FileInputStream(staged).getChannel().read(buf);
    Assert.assertArrayEquals("foo-bar-baz".getBytes(), buf.array());
  }

  @Test
  public void putArtifactBeforeNameFails() {
    byte[] data = "foo-".getBytes();
    RecordingStreamObserver<PutArtifactResponse> responseObserver = new RecordingStreamObserver<>();
    StreamObserver<PutArtifactRequest> requestObserver = stager.putArtifact(responseObserver);

    try {
      requestObserver.onNext(
          PutArtifactRequest.newBuilder()
              .setData(ArtifactChunk.newBuilder().setData(ByteString.copyFrom(data)).build())
              .build());
      Assert.fail("Should throw out of the call to 'onNext'");
    } catch (Exception ignored) {
    }
    Assert.assertThat(responseObserver.error, Matchers.not(Matchers.nullValue()));
  }

  @Test
  public void putArtifactWithNoContentFails() {
    RecordingStreamObserver<PutArtifactResponse> responseObserver = new RecordingStreamObserver<>();
    StreamObserver<PutArtifactRequest> requestObserver = stager.putArtifact(responseObserver);

    try {
      requestObserver.onNext(
          PutArtifactRequest.newBuilder().setData(ArtifactChunk.getDefaultInstance()).build());
      Assert.fail("Should throw out of the call to 'onNext'");
    } catch (Exception ignored) {
    }
    Assert.assertThat(responseObserver.error, Matchers.not(Matchers.nullValue()));
  }

  @Test
  public void commitManifestWithAllArtifactsSucceeds() {
    Artifact firstArtifact = stageBytes("first-artifact", "foo, bar, baz, quux".getBytes());
    Artifact secondArtifact = stageBytes("second-artifact", "spam, ham, eggs".getBytes());

    Manifest manifest =
        Manifest.newBuilder().addArtifact(firstArtifact).addArtifact(secondArtifact).build();

    RecordingStreamObserver<CommitManifestResponse> commitResponseObserver =
        new RecordingStreamObserver<>();
    stager.commitManifest(
        CommitManifestRequest.newBuilder().setManifest(manifest).build(), commitResponseObserver);
    Assert.assertThat(commitResponseObserver.completed, Matchers.is(true));
    Assert.assertThat(commitResponseObserver.responses, Matchers.hasSize(1));
    CommitManifestResponse commitResponse = commitResponseObserver.responses.get(0);
    Assert.assertThat(commitResponse.getStagingToken(), Matchers.not(Matchers.nullValue()));
  }

  @Test
  public void commitManifestWithMissingArtifactFails() {
    Artifact firstArtifact = stageBytes("first-artifact", "foo, bar, baz, quux".getBytes());
    Artifact absentArtifact = Artifact.newBuilder().setName("absent").build();

    Manifest manifest =
        Manifest.newBuilder().addArtifact(firstArtifact).addArtifact(absentArtifact).build();

    RecordingStreamObserver<CommitManifestResponse> commitResponseObserver =
        new RecordingStreamObserver<>();
    try {
      stager.commitManifest(CommitManifestRequest.newBuilder().setManifest(manifest).build(),
          commitResponseObserver);
      Assert.fail("commit should not complete");
    } catch (Exception e) {
      Assert.assertThat(commitResponseObserver.error, Matchers.not(Matchers.nullValue()));
    }
  }

  private Artifact stageBytes(String name, byte[] bytes) {
    StreamObserver<PutArtifactRequest> requests =
        stager.putArtifact(new RecordingStreamObserver<PutArtifactResponse>());
    requests.onNext(PutArtifactRequest.newBuilder().setName(name).build());
    requests.onNext(
        PutArtifactRequest.newBuilder()
            .setData(ArtifactChunk.newBuilder().setData(ByteString.copyFrom(bytes)).build())
            .build());
    requests.onCompleted();
    return Artifact.newBuilder().setName(name).build();
  }

  private static class RecordingStreamObserver<T> implements StreamObserver<T> {
    private List<T> responses = new ArrayList<>();
    @Nullable private Throwable error = null;
    private boolean completed = false;

    @Override
    public void onNext(T value) {
      failIfTerminal();
      responses.add(value);
    }

    @Override
    public void onError(Throwable t) {
      failIfTerminal();
      error = t;
    }

    @Override
    public void onCompleted() {
      failIfTerminal();
      completed = true;
    }

    private void failIfTerminal() {
      if (error != null || completed) {
        Assert.fail("Should have terminated after entering a terminal state");
      }
    }
  }
}
