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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.runners.core.construction.ArtifactServiceStager;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link LocalFileSystemArtifactRetrievalService}.
 */
@RunWith(JUnit4.class)
public class LocalFileSystemArtifactRetrievalServiceTest {
  @Rule public TemporaryFolder tmp = new TemporaryFolder();
  @Rule public ExpectedException thrown = ExpectedException.none();
  private GrpcFnServer<LocalFileSystemArtifactStagerService> stager;
  private GrpcFnServer<LocalFileSystemArtifactRetrievalService> retriever;
  private ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceStub retrievalStub;
  private File stagingRoot;

  private ArtifactServiceStager artifactStager;
  private InProcessServerFactory serverFactory;

  @Before
  public void setup() throws Exception {
    stagingRoot = tmp.newFolder();
    serverFactory = InProcessServerFactory.create();
    stager =
        GrpcFnServer.allocatePortAndCreateFor(
            LocalFileSystemArtifactStagerService.withRootDirectory(stagingRoot), serverFactory);
    artifactStager =
        ArtifactServiceStager.overChannel(
            InProcessChannelBuilder.forName(this.stager.getApiServiceDescriptor().getUrl())
                .build());
  }

  @After
  public void teardown() throws Exception {
    stager.close();
    retriever.close();
  }

  @Test
  public void retrieveManifest() throws Exception {
    Map<String, byte[]> artifacts = Collections.singletonMap("first", "foo, bar, baz".getBytes());
    stageArtifactsAndCreateRetriever(artifacts);

    final AtomicReference<Manifest> manifestRef = new AtomicReference<>();
    final CountDownLatch retrieved = new CountDownLatch(1);
    retrievalStub.getManifest(
        GetManifestRequest.getDefaultInstance(),
        new StreamObserver<GetManifestResponse>() {
          @Override
          public void onNext(GetManifestResponse value) {
            manifestRef.set(value.getManifest());
          }

          @Override
          public void onError(Throwable t) {
            retrieved.countDown();
          }

          @Override
          public void onCompleted() {
            retrieved.countDown();
          }
        });

    retrieved.await();
    Manifest manifest = manifestRef.get();
    assertThat(manifest, not(nullValue()));
    assertThat("Only one artifact should be staged", manifest.getArtifactCount(), equalTo(1));
    ArtifactMetadata artifactMetadata = manifest.getArtifact(0);
    assertThat(artifactMetadata.getName(), equalTo("first"));
  }

  @Test
  public void retrieveArtifact() throws Exception {
    String contents = "foo, bar, baz";
    Map<String, byte[]> artifacts = Collections.singletonMap("first", contents.getBytes());
    stageArtifactsAndCreateRetriever(artifacts);

    final CountDownLatch retrieved = new CountDownLatch(1);
    final List<ArtifactChunk> chunks = new ArrayList<>();
    retrievalStub.getArtifact(
        GetArtifactRequest.newBuilder().setName("first").build(),
        new StreamObserver<ArtifactChunk>() {
          @Override
          public void onNext(ArtifactChunk value) {
            synchronized (chunks) {
              chunks.add(value);
            }
          }

          @Override
          public void onError(Throwable t) {
            retrieved.countDown();
          }

          @Override
          public void onCompleted() {
            retrieved.countDown();
          }
        });

    retrieved.await();
    StringBuilder builder = new StringBuilder();
    synchronized (chunks) {
      for (ArtifactChunk chunk : chunks) {
        builder.append(new String(chunk.getData().toByteArray()));
      }
    }
    assertThat(builder.toString(), equalTo(contents));
  }

  @Test
  public void retrieveArtifactNotPresent() throws Exception {
    String contents = "foo, bar, baz";
    Map<String, byte[]> artifacts = Collections.singletonMap("first", contents.getBytes());
    stageArtifactsAndCreateRetriever(artifacts);

    final CountDownLatch retrieved = new CountDownLatch(1);
    final AtomicReference<Throwable> thrown = new AtomicReference<>();
    retrievalStub.getArtifact(
        GetArtifactRequest.newBuilder().setName("absent").build(),
        new StreamObserver<ArtifactChunk>() {
          @Override
          public void onNext(ArtifactChunk value) {
          }

          @Override
          public void onError(Throwable t) {
            thrown.set(t);
            retrieved.countDown();
          }

          @Override
          public void onCompleted() {
            retrieved.countDown();
          }
        });

    retrieved.await();
    assertThat("Attempting to retrieve an absent artifact should throw", thrown, not(nullValue()));
  }

  private void stageArtifactsAndCreateRetriever(Map<String, byte[]> artifacts) throws Exception {
    List<File> artifactFiles = new ArrayList<>();
    for (Map.Entry<String, byte[]> artifact : artifacts.entrySet()) {
      File artifactFile = tmp.newFile(artifact.getKey());
      try (FileChannel channel = new FileOutputStream(artifactFile).getChannel()) {
        channel.write(ByteBuffer.wrap(artifact.getValue()));
      }
    }
    artifactStager.stage(artifactFiles);

    retriever =
        GrpcFnServer.allocatePortAndCreateFor(
            LocalFileSystemArtifactRetrievalService.forRootDirectory(stagingRoot), serverFactory);
    retrievalStub =
        ArtifactRetrievalServiceGrpc.newStub(
            InProcessChannelBuilder.forName(retriever.getApiServiceDescriptor().getUrl()).build());
  }
}
