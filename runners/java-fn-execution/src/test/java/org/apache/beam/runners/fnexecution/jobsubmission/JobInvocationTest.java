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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.MoreExecutors;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests for {@link JobInvocation}. */
public class JobInvocationTest {

  private static ExecutorService executorService;

  private JobInvocation jobInvocation;
  private ControllablePipelineRunner runner;

  @BeforeClass
  public static void init() {
    executorService = Executors.newFixedThreadPool(1);
  }

  @AfterClass
  public static void shutdown() {
    executorService.shutdownNow();
    executorService = null;
  }

  @Before
  public void setup() {
    JobInfo jobInfo =
        JobInfo.create("jobid", "jobName", "retrievalToken", Struct.getDefaultInstance());
    ListeningExecutorService listeningExecutorService =
        MoreExecutors.listeningDecorator(executorService);
    Pipeline pipeline = Pipeline.create();
    runner = new ControllablePipelineRunner();
    jobInvocation =
        new JobInvocation(
            jobInfo, listeningExecutorService, PipelineTranslation.toProto(pipeline), runner);
  }

  @Test(timeout = 10_000)
  public void testStateAfterCompletion() throws Exception {
    jobInvocation.start();
    assertThat(jobInvocation.getState(), is(JobApi.JobState.Enum.RUNNING));

    TestPipelineResult pipelineResult = new TestPipelineResult(PipelineResult.State.DONE);
    runner.setResult(pipelineResult);

    awaitChangingJobState(jobInvocation, JobApi.JobState.Enum.RUNNING, JobApi.JobState.Enum.DONE);
  }

  @Test(timeout = 10_000)
  public void testStateAfterCompletionWithoutResult() throws Exception {
    jobInvocation.start();
    assertThat(jobInvocation.getState(), is(JobApi.JobState.Enum.RUNNING));

    // Let pipeline finish without a result.
    // Note that some Runners (e.g. Flink) only provide a result when the pipeline execution has
    // finished
    runner.setResult(null);

    awaitChangingJobState(
        jobInvocation, JobApi.JobState.Enum.RUNNING, JobApi.JobState.Enum.UNSPECIFIED);
  }

  @Test(timeout = 10_000)
  public void testStateAfterCancellation() throws Exception {
    jobInvocation.start();
    assertThat(jobInvocation.getState(), is(JobApi.JobState.Enum.RUNNING));

    jobInvocation.cancel();
    awaitChangingJobState(
        jobInvocation, JobApi.JobState.Enum.RUNNING, JobApi.JobState.Enum.CANCELLED);
  }

  @Test(timeout = 10_000)
  public void testStateAfterCancellationWithPipelineResult() throws Exception {
    jobInvocation.start();
    assertThat(jobInvocation.getState(), is(JobApi.JobState.Enum.RUNNING));

    TestPipelineResult pipelineResult = new TestPipelineResult(PipelineResult.State.RUNNING);
    runner.setResult(pipelineResult);

    jobInvocation.cancel();
    awaitChangingJobState(
        jobInvocation, JobApi.JobState.Enum.RUNNING, JobApi.JobState.Enum.CANCELLED);
    assertThat(pipelineResult.cancelCalled, is(true));
  }

  @Test(timeout = 10_000)
  public void testStateAfterCancellationWhenDone() throws Exception {
    jobInvocation.start();
    assertThat(jobInvocation.getState(), is(JobApi.JobState.Enum.RUNNING));

    TestPipelineResult pipelineResult = new TestPipelineResult(PipelineResult.State.DONE);
    runner.setResult(pipelineResult);

    jobInvocation.cancel();
    awaitChangingJobState(jobInvocation, JobApi.JobState.Enum.RUNNING, JobApi.JobState.Enum.DONE);
    assertThat(pipelineResult.cancelCalled, is(false));
  }

  private static void awaitChangingJobState(
      JobInvocation jobInvocation,
      JobApi.JobState.Enum currentState,
      JobApi.JobState.Enum desiredState)
      throws Exception {
    while (jobInvocation.getState() == currentState) {
      Thread.sleep(100);
    }
    assertThat(jobInvocation.getState(), is(desiredState));
  }

  private static class ControllablePipelineRunner implements PortablePipelineRunner {

    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile PipelineResult result;

    @Override
    public PipelineResult run(RunnerApi.Pipeline pipeline, JobInfo jobInfo) {
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return result;
    }

    void setResult(PipelineResult pipelineResult) {
      result = pipelineResult;
      latch.countDown();
    }
  }

  private static class TestPipelineResult implements PipelineResult {

    private final State state;
    private volatile boolean cancelCalled;

    private TestPipelineResult(State state) {
      this.state = state;
    }

    @Override
    public State getState() {
      return state;
    }

    @Override
    public State cancel() {
      cancelCalled = true;
      return State.CANCELLED;
    }

    @Override
    public State waitUntilFinish(Duration duration) {
      return null;
    }

    @Override
    public State waitUntilFinish() {
      return null;
    }

    @Override
    public MetricResults metrics() {
      return null;
    }
  }
}
