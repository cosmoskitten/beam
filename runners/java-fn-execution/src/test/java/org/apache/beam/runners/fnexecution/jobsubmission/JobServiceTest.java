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

import com.google.protobuf.Struct;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingService;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingServiceProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link JobService}. */
@RunWith(JUnit4.class)
public class JobServiceTest {
  private static final String TEST_JOB_NAME = "test-job";

  @Mock
  GrpcFnServer<ArtifactStagingService> artifactStagingServer;
  @Mock
  JobInvoker invoker;

  JobService service;
  GrpcFnServer<JobService> server;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    ArtifactStagingServiceProvider provider = ignored -> artifactStagingServer;
    service = JobService.create(provider, invoker);
    server = GrpcFnServer.allocatePortAndCreateFor(service, InProcessServerFactory.create());
  }

  @After
  public void tearDown() throws Exception {
    server.close();
  }

  @Test
  public void testJobSubmissionUsesJobInvoker() throws Exception {
  }

  private JobApi.PrepareJobRequest createTestJobRequest() {
    return JobApi.PrepareJobRequest.newBuilder()
        .setJobName(TEST_JOB_NAME)
        .setPipeline(RunnerApi.Pipeline.getDefaultInstance())
        .setPipelineOptions(Struct.getDefaultInstance())
        .build();
  }
}
