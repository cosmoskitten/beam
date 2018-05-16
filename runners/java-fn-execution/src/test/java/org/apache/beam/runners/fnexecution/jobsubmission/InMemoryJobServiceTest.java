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

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.Struct;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link InMemoryJobService}. */
@RunWith(JUnit4.class)
public class InMemoryJobServiceTest {
  private static final RunnerApi.Pipeline TEST_PIPELINE = RunnerApi.Pipeline.getDefaultInstance();
  private static final String TEST_JOB_ID = "test-job-id";
  private static final String TEST_JOB_NAME = "test-job";
  private static final String TEST_STAGING_TOKEN = "test-staging-token";
  private static final Struct TEST_OPTIONS = Struct.getDefaultInstance();

  Endpoints.ApiServiceDescriptor stagingServiceDescriptor;
  @Mock
  JobInvoker invoker;
  @Mock
  JobInvocation invocation;

  InMemoryJobService service;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    stagingServiceDescriptor = Endpoints.ApiServiceDescriptor.getDefaultInstance();
    service = InMemoryJobService.create(stagingServiceDescriptor, invoker);
    when(invoker.invoke(TEST_PIPELINE, TEST_OPTIONS, TEST_STAGING_TOKEN)).thenReturn(invocation);
    when(invocation.getId()).thenReturn(TEST_JOB_ID);
  }

  @Test
  public void testPrepareIsSuccessful() {
    JobApi.PrepareJobRequest request =
        JobApi.PrepareJobRequest
            .newBuilder()
            .setJobName(TEST_JOB_NAME)
            .setPipeline(RunnerApi.Pipeline.getDefaultInstance())
            .setPipelineOptions(Struct.getDefaultInstance())
            .build();
    RecordingObserver<JobApi.PrepareJobResponse> recorder = new RecordingObserver<>();
    service.prepare(request, recorder);
    assertThat(recorder.isSuccessful(), is(true));
    assertThat(recorder.values, hasSize(1));
    JobApi.PrepareJobResponse response = recorder.values.get(0);
    assertThat(response.getArtifactStagingEndpoint(), notNullValue());
    assertThat(response.getPreparationId(), notNullValue());
  }

  @Test
  public void testJobSubmissionUsesJobInvokerAndIsSuccess() throws Exception {
    // prepare job
    JobApi.PrepareJobRequest prepareRequest =
        JobApi.PrepareJobRequest.newBuilder()
            .setJobName(TEST_JOB_NAME)
            .setPipeline(RunnerApi.Pipeline.getDefaultInstance())
            .setPipelineOptions(Struct.getDefaultInstance())
            .build();
    RecordingObserver<JobApi.PrepareJobResponse> prepareRecorder = new RecordingObserver<>();
    service.prepare(prepareRequest, prepareRecorder);
    JobApi.PrepareJobResponse prepareResponse = prepareRecorder.values.get(0);
    // run job
    JobApi.RunJobRequest runRequest =
        JobApi.RunJobRequest.newBuilder()
            .setPreparationId(prepareResponse.getPreparationId())
            .setStagingToken(TEST_STAGING_TOKEN)
            .build();
    RecordingObserver<JobApi.RunJobResponse> runRecorder = new RecordingObserver<>();
    service.run(runRequest, runRecorder);
    verify(invoker, times(1)).invoke(TEST_PIPELINE, TEST_OPTIONS, TEST_STAGING_TOKEN);
    assertThat(runRecorder.isSuccessful(), is(true));
    assertThat(runRecorder.values, hasSize(1));
    JobApi.RunJobResponse runResponse = runRecorder.values.get(0);
    assertThat(runResponse.getJobId(), is(TEST_JOB_ID));
  }

  @Test
  public void testGetStateForwardsStateFromJobInvocation() throws Exception {
    // set up mock
    when(invocation.getState()).thenReturn(JobApi.JobState.Enum.RUNNING);
    // prepare job
    JobApi.PrepareJobRequest prepareRequest =
        JobApi.PrepareJobRequest.newBuilder()
            .setJobName(TEST_JOB_NAME)
            .setPipeline(RunnerApi.Pipeline.getDefaultInstance())
            .setPipelineOptions(Struct.getDefaultInstance())
            .build();
    RecordingObserver<JobApi.PrepareJobResponse> prepareRecorder = new RecordingObserver<>();
    service.prepare(prepareRequest, prepareRecorder);
    String prepId = prepareRecorder.values.get(0).getPreparationId();
    // run job
    JobApi.RunJobRequest runRequest =
        JobApi.RunJobRequest.newBuilder()
            .setPreparationId(prepId)
            .setStagingToken(TEST_STAGING_TOKEN)
            .build();
    RecordingObserver<JobApi.RunJobResponse> runRecorder = new RecordingObserver<>();
    service.run(runRequest, runRecorder);
    String jobId = runRecorder.values.get(0).getJobId();

    // get state
    JobApi.GetJobStateRequest stateRequest =
        JobApi.GetJobStateRequest.newBuilder()
        .setJobId(jobId)
        .build();
    RecordingObserver<JobApi.GetJobStateResponse> stateRecorder = new RecordingObserver<>();
    service.getState(stateRequest, stateRecorder);
    verify(invocation, times(1)).getState();
    assertThat(stateRecorder.isSuccessful(), is(true));
    assertThat(stateRecorder.values, hasSize(1));
    assertThat(stateRecorder.values.get(0).getState(), is(Enum.RUNNING));
  }

  private static class RecordingObserver<T> implements StreamObserver<T> {
    ArrayList<T> values = new ArrayList<>();
    Throwable error = null;
    boolean isCompleted = false;

    @Override
    public void onNext(T t) {
      values.add(t);
    }

    @Override
    public void onError(Throwable throwable) {
      error = throwable;
    }

    @Override
    public void onCompleted() {
      isCompleted = true;
    }

    boolean isSuccessful() {
      return isCompleted && error == null;
    }
  }
}
