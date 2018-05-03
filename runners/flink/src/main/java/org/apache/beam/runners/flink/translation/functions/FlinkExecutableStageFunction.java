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
package org.apache.beam.runners.flink.translation.functions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.protobuf.Struct;
import java.util.Map;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

// TODO: https://issues.apache.org/jira/browse/BEAM-2597 Implement this executable stage operator.
/**
 * Flink operator that passes its input DataSet through an SDK-executed {@link
 * org.apache.beam.runners.core.construction.graph.ExecutableStage}.
 *
 * <p>The output of this operation is a multiplexed DataSet whose elements are tagged with a union
 * coder. The coder's tags are determined by the output coder map. The resulting data set should be
 * further processed by a {@link FlinkExecutableStagePruningFunction}.
 */
public class FlinkExecutableStageFunction<InputT>
    extends RichMapPartitionFunction<WindowedValue<InputT>, RawUnionValue> {

  // The executable stage this function will run.
  private final RunnerApi.ExecutableStagePayload stagePayload;
  // Pipeline options. Used for provisioning api.
  private final Struct pipelineOptions;
  // Map from PCollection id to the union tag used to represent this PCollection in the output.
  private final Map<String, Integer> outputMap;

  // Worker-local fields. These should only be constructed and consumed on Flink TaskManagers.
  private transient RuntimeContext runtimeContext;
  private transient StateRequestHandler stateRequestHandler;
  private transient StageBundleFactory stageBundleFactory;

  public FlinkExecutableStageFunction(
      RunnerApi.ExecutableStagePayload stagePayload,
      Struct pipelineOptions,
      Map<String, Integer> outputMap) {
    this.stagePayload = stagePayload;
    this.pipelineOptions = pipelineOptions;
    this.outputMap = outputMap;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    ExecutableStage executableStage = ExecutableStage.fromPayload(stagePayload);
    runtimeContext = getRuntimeContext();
    // NOTE: It's safe to reuse the state handler between partitions because each partition uses the
    // same backing runtime context and broadcast variables. We use checkState below to catch errors
    // in backward-incompatible Flink changes.
    stateRequestHandler = FlinkBatchStateRequestHandler.forStage(executableStage, runtimeContext);
    // TODO: Implement and wire in JobBundleFactory.
    JobBundleFactory jobBundleFactory = null;
    stageBundleFactory = jobBundleFactory.forStage(executableStage);
  }

  @Override
  public void mapPartition(
      Iterable<WindowedValue<InputT>> iterable, Collector<RawUnionValue> collector)
      throws Exception {
    checkState(
        runtimeContext == getRuntimeContext(),
        "RuntimeContext changed from under us. State handler invalid.");
    checkState(
        stageBundleFactory != null, "%s not yet prepared", StageBundleFactory.class.getName());
    checkState(
        stateRequestHandler != null, "%s not yet prepared", StateRequestHandler.class.getName());

    try (RemoteBundle<InputT> bundle =
        stageBundleFactory.getBundle(
            new ReceiverFactory(collector, outputMap), stateRequestHandler)) {
      FnDataReceiver<WindowedValue<InputT>> receiver = bundle.getInputReceiver();
      for (WindowedValue<InputT> input : iterable) {
        receiver.accept(input);
      }
    }
    // NOTE: RemoteBundle.close() blocks on completion of all data receivers. This is necessary to
    // safely reference the partition-scoped Collector from receivers.
  }

  @Override
  public void close() throws Exception {
    stageBundleFactory.close();
  }

  private static class ReceiverFactory implements OutputReceiverFactory {

    private final Object collectorLock = new Object();

    @GuardedBy("collectorLock")
    private final Collector<RawUnionValue> collector;

    private final Map<String, Integer> outputMap;

    ReceiverFactory(Collector<RawUnionValue> collector, Map<String, Integer> outputMap) {
      this.collector = collector;
      this.outputMap = outputMap;
    }

    @Override
    public <OutputT> FnDataReceiver<OutputT> create(String collectionId) {
      Integer unionTag = outputMap.get(collectionId);
      checkArgument(unionTag != null, "Unknown PCollection id: %s", collectionId);
      int tagInt = unionTag;
      // TODO: Synchronize access to collector.
      return (receivedElement) -> {
        synchronized (collectorLock) {
          collector.collect(new RawUnionValue(tagInt, receivedElement));
        }
      };
    }
  }
}
