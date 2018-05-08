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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse.Builder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * {@link StateRequestHandler} that uses a Flink {@link RuntimeContext} to access Flink broadcast
 * variable that represent side inputs.
 */
class FlinkBatchStateRequestHandler implements StateRequestHandler {

  // Map from side input id to global PCollection id.
  private final Map<SideInputId, PCollectionNode> sideInputToCollection;
  // Components used for the executable stage this request handler will be handling.
  private final Components components;
  // Flink runtime context (for access to broadcase variables).
  private final RuntimeContext runtimeContext;

  /**
   * Creates a new state handler for the given stage. Note that this requires a traversal of the
   * stage itself, so this should only be called once per stage rather than once per bundle.
   */
  static FlinkBatchStateRequestHandler forStage(
      ExecutableStage stage, RuntimeContext runtimeContext) {
    ImmutableMap.Builder<SideInputId, PCollectionNode> sideInputBuilder = ImmutableMap.builder();
    for (SideInputReference sideInput : stage.getSideInputs()) {
      sideInputBuilder.put(
          SideInputId.newBuilder()
              .setTransformId(sideInput.transform().getId())
              .setLocalName(sideInput.localName())
              .build(),
          sideInput.collection());
    }
    Components components = stage.getComponents();
    return new FlinkBatchStateRequestHandler(sideInputBuilder.build(), components, runtimeContext);
  }

  private FlinkBatchStateRequestHandler(
      Map<SideInputId, PCollectionNode> sideInputToCollection,
      Components components,
      RuntimeContext runtimeContext) {
    this.sideInputToCollection = sideInputToCollection;
    this.components = components;
    this.runtimeContext = runtimeContext;
  }

  @Override
  public CompletionStage<Builder> handle(BeamFnApi.StateRequest request) throws Exception {
    throw new UnsupportedOperationException();
  }
}
