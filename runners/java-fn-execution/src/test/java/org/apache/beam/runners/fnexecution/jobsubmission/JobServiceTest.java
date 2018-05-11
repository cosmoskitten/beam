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
package org.apache.beam.runners.fnexecution.jobsubmission;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.Struct;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.RunJobResponse;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingService;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingServiceProvider;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link JobService}. */
@RunWith(JUnit4.class)
public class JobServiceTest {
  private static final String TEST_JOB_NAME = "test-job";
  private static final String STAGING_TOKEN = "staging-token";
  private static final String INVOCATION_ID = "invocation-id";

  @Mock
  GrpcFnServer<ArtifactStagingService> artifactStagingServer;
  private static final ApiServiceDescriptor artifactStagingDescriptor =
      ApiServiceDescriptor.getDefaultInstance();
  @Mock
  ArtifactStagingServiceProvider artifactStagingServerProvider;
  @Mock
  JobInvoker invoker;
  @Mock
  JobInvocation invocation;

  JobService service;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(artifactStagingServer.getApiServiceDescriptor()).thenReturn(artifactStagingDescriptor);
    when(artifactStagingServerProvider.forJob(any())).thenReturn(artifactStagingServer);
    when(invoker.invoke(any(), any())).thenReturn(invocation);
    when(invocation.getId()).thenReturn(INVOCATION_ID);
    service = JobService.create(artifactStagingServerProvider, invoker);
  }

  @Test
  public void testPrepareCreatesArtifactStagingService() throws Exception {
    StreamObserver<PrepareJobResponse> responseObserver = mock(StreamObserver.class);
    PrepareJobRequest request = createTestJobPrepareRequest();
    service.prepare(request, responseObserver);
    verify(artifactStagingServerProvider, times(1)).forJob(any());
  }

  @Test
  public void testPrepareResponseContainsExpectedValues() throws Exception {
    StreamRecorder<PrepareJobResponse> responseObserver = new StreamRecorder<>();
    PrepareJobRequest request = createTestJobPrepareRequest();
    service.prepare(request, responseObserver);
    assertTrue(responseObserver.isSuccess());
    assertThat(responseObserver.values, hasSize(1));
    assertThat(
        responseObserver.values.get(0).getArtifactStagingEndpoint(),
        equalTo(artifactStagingDescriptor));
    assertThat(responseObserver.values.get(0).getPreparationId(), notNullValue());
  }

  @Test
  public void testRunCallsInvokerWithCorrectJobPreparation() throws Exception {
    PrepareJobResponse prepResponse = prepareExampleJob();
    RunJobRequest runRequest = createTestJobRunRequest(prepResponse);
    StreamObserver<RunJobResponse> responseObserver = mock(StreamObserver.class);
    service.run(runRequest, responseObserver);
    ArgumentCaptor<JobPreparation> prepCaptor = ArgumentCaptor.forClass(JobPreparation.class);
    verify(invoker, times(1)).invoke(prepCaptor.capture(), any());
    assertThat(prepCaptor.getValue().id(), equalTo(prepResponse.getPreparationId()));
  }

  @Test
  public void testRunContainsExpectedValue() throws Exception {
    PrepareJobResponse prepResponse = prepareExampleJob();
    RunJobRequest runRequest = createTestJobRunRequest(prepResponse);
    StreamRecorder<RunJobResponse> responseObserver = new StreamRecorder<>();
    service.run(runRequest, responseObserver);
    assertTrue(responseObserver.isSuccess());
    assertThat(responseObserver.values, hasSize(1));
    assertThat(responseObserver.values.get(0).getJobId(), notNullValue());
  }

  private JobApi.PrepareJobRequest createTestJobPrepareRequest() {
    return JobApi.PrepareJobRequest.newBuilder()
        .setJobName(TEST_JOB_NAME)
        .setPipeline(RunnerApi.Pipeline.getDefaultInstance())
        .setPipelineOptions(Struct.getDefaultInstance())
        .build();
  }

  private JobApi.RunJobRequest createTestJobRunRequest(PrepareJobResponse prepareJobResponse) {
    return JobApi.RunJobRequest.newBuilder()
        .setPreparationId(prepareJobResponse.getPreparationId())
        .setStagingToken(STAGING_TOKEN)
        .build();
  }

  private PrepareJobResponse prepareExampleJob() throws Exception {
    StreamRecorder<PrepareJobResponse> responseObserver = new StreamRecorder<>();
    PrepareJobRequest request = createTestJobPrepareRequest();
    service.prepare(request, responseObserver);
    return responseObserver.values.get(0);
  }

  private static class StreamRecorder<T> implements StreamObserver<T> {
    final ArrayList<T> values = new ArrayList<>();
    Throwable error = null;
    boolean completed = false;

    @Override
    public void onNext(T t) {
      values.add(t);
    }

    @Override
    public void onError(Throwable throwable) {
      this.error = throwable;
    }

    @Override
    public void onCompleted() {
      completed = true;
    }

    public boolean isSuccess() {
      return completed && error == null;
    }
  }


}
