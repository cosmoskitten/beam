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
package org.apache.beam.runners.flink;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.flink.metrics.FlinkMetricResults;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.joda.time.Duration;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

/**
 * A {@link FlinkStreamingPipelineJob} represents a job submitted via
 * {@link FlinkStreamingPipelineExecutor}.
 *
 * <p>We use our own code for Job submission and tracking because Flink
 * {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment} does not support
 * non-blocking job submission.
 */
abstract class FlinkStreamingPipelineJob implements PipelineResult {

  private static final Map<JobStatus, State> FLINK_STATE_TO_JOB_STATE =
      ImmutableMap
          .<JobStatus, State>builder()
          .put(JobStatus.CANCELLING, State.CANCELLED)
          .put(JobStatus.CANCELED, State.CANCELLED)
          .put(JobStatus.CREATED, State.RUNNING)
          .put(JobStatus.FAILING, State.FAILED)
          .put(JobStatus.FAILED, State.FAILED)
          .put(JobStatus.RESTARTING, State.RUNNING)
          .put(JobStatus.RUNNING, State.RUNNING)
          .put(JobStatus.FINISHED, State.DONE)
          .put(JobStatus.SUSPENDED, State.STOPPED)
          .build();

  protected abstract Configuration getConfiguration();
  protected abstract FiniteDuration getClientTimeout();
  protected abstract JobID getJobId();

  @Override
  public State getState() {
    StandaloneClusterClient clusterClient = null;
    ActorGateway jobManagerGateway;
    try {
      try {
        clusterClient = new StandaloneClusterClient(getConfiguration());
        jobManagerGateway = clusterClient.getJobManagerGateway();

      } catch (Exception e) {
        throw new RuntimeException("Error retrieving cluster client.", e);
      }


      Future<Object> response = jobManagerGateway.ask(
          JobManagerMessages.getRequestJobStatus(getJobId()),
          getClientTimeout());

      Object result;
      try {
        result = Await.result(response, getClientTimeout());
      } catch (Exception e) {
        throw new RuntimeException("Could not retrieve Job status from JobManager.", e);
      }

      if (result instanceof JobManagerMessages.JobNotFound) {
        return State.UNKNOWN;
      } else if (result instanceof JobManagerMessages.CurrentJobStatus) {
        return toState(((JobManagerMessages.CurrentJobStatus) result).status());
      }
    } finally {
      if (clusterClient != null) {
        clusterClient.shutdown();
      }
    }
    return State.UNKNOWN;
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    final FiniteDuration testTimeout =
        new FiniteDuration(duration.getMillis(), TimeUnit.MILLISECONDS);
    final Deadline deadline = testTimeout.fromNow();

    while (deadline.hasTimeLeft()) {
      State state = getState();

      switch (state) {
        case RUNNING:
        case UNKNOWN:
          continue;
        case FAILED:
        case CANCELLED:
        case DONE:
        case STOPPED:
        case UPDATED:
          return state;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    return State.UNKNOWN;
  }

  @Override
  public State waitUntilFinish() {
    while (true) {
      State state = getState();

      switch (state) {
        case RUNNING:
        case UNKNOWN:
          continue;
        case FAILED:
        case CANCELLED:
        case DONE:
        case STOPPED:
        case UPDATED:
          return state;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }

  @Override
  public MetricResults metrics() {
    StandaloneClusterClient clusterClient;
    try {
      clusterClient = new StandaloneClusterClient(getConfiguration());
    } catch (Exception e) {
      throw new RuntimeException("Error retrieving cluster client.", e);
    }

    try {
      Map<String, Object> accumulators = clusterClient.getAccumulators(getJobId());
      return new FlinkMetricResults(accumulators);

    } catch (Exception e) {
      throw new RuntimeException("Could not retrieve Accumulators from JobManager.", e);
    }
  }

  private static State toState(JobStatus flinkStatus) {
    return MoreObjects.firstNonNull(FLINK_STATE_TO_JOB_STATE.get(flinkStatus), State.UNKNOWN);
  }
}
