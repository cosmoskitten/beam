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
