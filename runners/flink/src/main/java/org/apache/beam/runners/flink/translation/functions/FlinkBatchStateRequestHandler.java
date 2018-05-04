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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse.Builder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * {@link StateRequestHandler} that uses a Flink {@link RuntimeContext} to access Flink broadcast
 * variable that represent side inputs.
 */
class FlinkBatchStateRequestHandler implements StateRequestHandler {

  // Map from side input id to global PCollection id.
  private final Map<SideInputId, PCollectionNode> sideInputToCollection;
  private final Components components;
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
    if (request.getStateKey().getTypeCase() != BeamFnApi.StateKey.TypeCase.MULTIMAP_SIDE_INPUT) {
      throw new UnsupportedOperationException(
          "This handler can only respond to MULTIMAP_SIDE_INPUT request.");
    }
    BeamFnApi.StateKey.MultimapSideInput multimapSideInput =
        request.getStateKey().getMultimapSideInput();

    String transformId = multimapSideInput.getPtransformId();
    String localName = multimapSideInput.getSideInputId();

    PCollectionNode collectionNode =
        sideInputToCollection.get(
            SideInputId.newBuilder().setTransformId(transformId).setLocalName(localName).build());
    checkState(collectionNode != null, "No side input for %s/%s", transformId, localName);
    List<Object> broadcastVariable = runtimeContext.getBroadcastVariable(collectionNode.getId());

    // TODO: Ensure that PCollections referenced within ProcessBundleDescriptors use length
    // prefixing when consumed as side inputs. Alternatively, provide a new WireCoders mechanism
    // to instantiate the original PCollection coder _without_ inserting length prefixes.
    Coder javaCoder = WireCoders.instantiateRunnerWireCoder(collectionNode, components);
    // TODO: What is the protocol for state api coders? If we're only answering multimap requests,
    // can we _always_ use KV<byte[], byte[]>?

    // TODO: we wouldn't have to do this if the harness didn't always
    // expect a KV<Void, T> as input for a side input, currently the key field
    // in the MultimapSideInput request is always Void
    // we know the input coder is always a WindowedValueCoder<KvCoder<Void, SomeCoder>>
    // and the harness expects a list of things encoded with SomeCoder
    KvCoder kvCoder = (KvCoder) ((WindowedValueCoder) javaCoder).getValueCoder();
    Coder someCoder = kvCoder.getValueCoder();

    ByteString.Output output = ByteString.newOutput();
    for (Object windowedValue : broadcastVariable) {
      Object value = ((KV) ((WindowedValue) windowedValue).getValue()).getValue();
      someCoder.encode(value, output);
    }

    CompletableFuture<Builder> response = new CompletableFuture<>();
    Builder responseBuilder =
        BeamFnApi.StateResponse.newBuilder()
            .setGet(BeamFnApi.StateGetResponse.newBuilder().setData(output.toByteString()));
    response.complete(responseBuilder);
    return response;
  }
}
