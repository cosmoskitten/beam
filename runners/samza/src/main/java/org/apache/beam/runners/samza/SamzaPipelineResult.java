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
package org.apache.beam.runners.samza;

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;

import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.runtime.ApplicationRunner;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The result from executing a Samza Pipeline. */
public class SamzaPipelineResult implements PipelineResult {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaPipelineResult.class);

  private final SamzaExecutionContext executionContext;
  private final ApplicationRunner runner;
  private final StreamApplication app;

  public SamzaPipelineResult(
      StreamApplication app, ApplicationRunner runner, SamzaExecutionContext executionContext) {
    this.executionContext = executionContext;
    this.runner = runner;
    this.app = app;
  }

  @Override
  public State getState() {
    return getStateInfo().state;
  }

  @Override
  public State cancel() {
    runner.kill();
    return waitUntilFinish();
  }

  @Override
  public State waitUntilFinish(@Nullable Duration duration) {
    try {
      if (duration == null) {
        runner.waitForFinish();
      } else {
        runner.waitForFinish(java.time.Duration.ofMillis(duration.getMillis()));
      }
    } catch (Exception e) {
      throw new Pipeline.PipelineExecutionException(e);
    }

    final StateInfo stateInfo = getStateInfo();
    if (stateInfo.state == State.FAILED) {
      throw stateInfo.error;
    }

    return stateInfo.state;
  }

  @Override
  public State waitUntilFinish() {
    return waitUntilFinish(null);
  }

  @Override
  public MetricResults metrics() {
    return asAttemptedOnlyMetricResults(executionContext.getMetricsContainer().getContainers());
  }

  private StateInfo getStateInfo() {
    final ApplicationStatus status = runner.status();
    switch (status.getStatusCode()) {
      case New:
        return new StateInfo(State.STOPPED);
      case Running:
        return new StateInfo(State.RUNNING);
      case SuccessfulFinish:
        return new StateInfo(State.DONE);
      case UnsuccessfulFinish:
        LOG.error(status.getThrowable().getMessage(), status.getThrowable());
        return new StateInfo(
            State.FAILED,
            new Pipeline.PipelineExecutionException(getUserCodeException(status.getThrowable())));
      default:
        return new StateInfo(State.UNKNOWN);
    }
  }

  private static class StateInfo {
    private final State state;
    private final Pipeline.PipelineExecutionException error;

    private StateInfo(State state) {
      this(state, null);
    }

    private StateInfo(State state, Pipeline.PipelineExecutionException error) {
      this.state = state;
      this.error = error;
    }
  }

  /**
   * Some of the Beam unit tests relying on the exception message to do assertion. This function
   * will find the original UserCodeException so the message will be exposed directly.
   */
  private static Throwable getUserCodeException(Throwable throwable) {
    Throwable t = throwable;
    while (t != null) {
      if (t instanceof UserCodeException) {
        return t;
      }

      t = t.getCause();
    }

    return throwable;
  }
}
