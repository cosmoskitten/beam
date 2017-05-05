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

import java.io.IOException;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

/**
 * A {@link FlinkStreamingPipelineJob} that runs on a local {@link LocalFlinkMiniCluster}.
 */
class FlinkLocalStreamingPipelineJob extends FlinkStreamingPipelineJob {

  private static final Logger LOG =
      LoggerFactory.getLogger(FlinkLocalStreamingPipelineJob.class);


  private final LocalFlinkMiniCluster flinkMiniCluster;
  private final JobID jobId;
  private final FiniteDuration clientTimeout = FiniteDuration.apply(10, "seconds");

  /**
   * We keep track of this so that the query methods can cancel early because when the job is done
   * the LocalFlinkMiniCluster does not allow any more querying.
   */
  private boolean cancelled = false;

  public FlinkLocalStreamingPipelineJob(
      FlinkPipelineOptions pipelineOptions,
      LocalStreamEnvironment flinkEnv) throws Exception {

    // transform the streaming program into a JobGraph
    StreamGraph streamGraph = flinkEnv.getStreamGraph();
    streamGraph.setJobName(pipelineOptions.getJobName());

    JobGraph jobGraph = streamGraph.getJobGraph();

    Configuration configuration = new Configuration();
    configuration.addAll(jobGraph.getJobConfiguration());

    configuration.setInteger(
        ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

    if (LOG.isInfoEnabled()) {
      LOG.info("Running job on local embedded Flink mini cluster");
    }

    flinkMiniCluster = new LocalFlinkMiniCluster(configuration, false);
    flinkMiniCluster.start();

    jobId = flinkMiniCluster.submitJobDetached(jobGraph).getJobID();
  }

  @Override
  public State cancel() throws IOException {
    flinkMiniCluster.stop();
    cancelled = true;
    return State.CANCELLED;
  }

  @Override
  public State getState() {
    if (cancelled) {
      return State.CANCELLED;
    }
    return super.getState();
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    if (cancelled) {
      return State.CANCELLED;
    }
    return super.waitUntilFinish(duration);
  }

  @Override
  public State waitUntilFinish() {
    if (cancelled) {
      return State.CANCELLED;
    }
    return super.waitUntilFinish();
  }

  @Override
  public MetricResults metrics() {
    if (cancelled) {
      throw new UnsupportedOperationException("Cannot query metrics after the job was cancelled.");
    }
    return super.metrics();
  }

  @Override
  protected Configuration getConfiguration() {
    return flinkMiniCluster.configuration();
  }

  @Override
  protected FiniteDuration getClientTimeout() {
    return clientTimeout;
  }

  @Override
  public JobID getJobId() {
    return jobId;
  }
}
