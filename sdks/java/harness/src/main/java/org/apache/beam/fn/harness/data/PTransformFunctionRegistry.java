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
package org.apache.beam.fn.harness.data;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.fn.harness.SimpleExecutionState;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;

/**
 * A class to to register and retrieve functions for bundle processing (i.e. the start, or finish
 * function). The purpose of this class is to wrap these functions with instrumentation for metrics
 * and other telemetry collection.
 *
 * <p>Usage: // Instantiate and use the registry for each class of functions. i.e. start. finish.
 *
 * <pre>
 * PTransformFunctionRegistry startFunctionRegistry;
 * PTransformFunctionRegistry finishFunctionRegistry;
 * startFunctionRegistry.register(myStartThrowingRunnable);
 * finishFunctionRegistry.register(myFinishThrowingRunnable);
 *
 * // Then invoke the functions by iterating over them, in your desired order: i.e.
 * for (ThrowingRunnable startFunction : startFunctionRegistry.getFunctions()) {
 *   startFunction.run();
 * }
 *
 * for (ThrowingRunnable finishFunction : Lists.reverse(finishFunctionRegistry.getFunctions())) {
 *   finishFunction.run();
 * }
 * // Note: this is used in ProcessBundleHandler.
 * </pre>
 */
public class PTransformFunctionRegistry {

  private List<ThrowingRunnable> runnables = new ArrayList<>();
  private List<SimpleExecutionState> executionStates = new ArrayList<SimpleExecutionState>();
  private MetricsContainerStepMap metricsContainerRegistry;
  private ExecutionStateTracker stateTracker;
  private String stateName;

  public PTransformFunctionRegistry(
      MetricsContainerStepMap metricsContainerRegistry,
      ExecutionStateTracker stateTracker,
      String stateName) {
    this.metricsContainerRegistry = metricsContainerRegistry;
    this.stateName = stateName;
    this.stateTracker = stateTracker;
    this.stateName = stateName;
  }

  /**
   * Register the runnable to process the specific pTransformId.
   *
   * @param pTransformId
   * @param runnable
   */
  public void register(String pTransformId, ThrowingRunnable runnable) {
    HashMap<String, String> labelsMetadata = new HashMap<String, String>();
    // TODO get this in a proper way, reuse method from simple monitoring info builder?
    labelsMetadata.put(SimpleMonitoringInfoBuilder.PTRANSFORM_LABEL, pTransformId);
    SimpleExecutionState state = new SimpleExecutionState(this.stateName, labelsMetadata);
    executionStates.add(state);

    // TODO(ajamato): RM, This is just here to make findbugs go away
    if (metricsContainerRegistry != null) {
      System.out.print("RM this");
    }

    ThrowingRunnable wrapped =
        () -> {
          // TODO(ajamato): Setup the proper pTransform context for Metrics to use.
          // TODO(ajamato): Set the proper state sampler state for ExecutionTime Metrics to use.
          try (Closeable close = this.stateTracker.enterState(state)) {
            runnable.run();
          }
        };
    runnables.add(wrapped);
  }

  public List<MonitoringInfo> getMonitoringInfos() {
    List<MonitoringInfo> monitoringInfos = new ArrayList<MonitoringInfo>();
    for (SimpleExecutionState state : executionStates) {
      SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder(false);
      // TODO implement them all
      if (this.stateName.equals("start")) {
        builder.setUrn(SimpleMonitoringInfoBuilder.START_BUNDLE_MSECS_URN);
      } else if (this.stateName.equals("finish")) {
        builder.setUrn(SimpleMonitoringInfoBuilder.FINISH_BUNDLE_MSECS_URN);
      }
      for (Map.Entry<String, String> entry : state.getLabels().entrySet()) {
        builder.setLabel(entry.getKey(), entry.getValue());
      }
      builder.setInt64Value(state.getTotalMillis());
      monitoringInfos.add(builder.build());
    }
    return monitoringInfos;
  }

  /**
   * @return A list of wrapper functions which will invoke the registered functions indirectly. The
   *     order of registry is maintained.
   */
  public List<ThrowingRunnable> getFunctions() {
    return runnables;
  }
}
