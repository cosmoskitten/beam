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
package org.apache.beam.runners.dataflow.worker.fn.control;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RegisterRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateAppendResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateClearResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest.RequestCase;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.dataflow.worker.ByteStringCoder;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Operation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.grpc.v1_13_1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1_13_1.com.google.protobuf.TextFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link Operation} is responsible for communicating with the SDK harness and asking it to
 * process a bundle of work. This operation registers the {@link
 * org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor} when executed the first
 * time. Afterwards, it only asks the SDK harness to process the bundle using the already registered
 * {@link org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor}.
 *
 * <p>This operation supports restart.
 */
public class RegisterAndProcessBundleOperation extends Operation {
  private static final Logger LOG =
      LoggerFactory.getLogger(RegisterAndProcessBundleOperation.class);

  private static final OutputReceiver[] EMPTY_RECEIVERS = new OutputReceiver[0];

  private final Supplier<String> idGenerator;
  private final InstructionRequestHandler instructionRequestHandler;
  private final StateDelegator beamFnStateDelegator;
  private final RegisterRequest registerRequest;
  private final Map<String, DataflowStepContext> ptransformIdToUserStepContext;
  private final Map<String, SideInputReader> ptransformIdToSideInputReader;
  private final Table<String, String, PCollectionView<?>>
      ptransformIdToSideInputIdToPCollectionView;
  private final ConcurrentHashMap<StateKey, BagState<ByteString>> userStateData;

  private @Nullable CompletionStage<InstructionResponse> registerFuture;
  private @Nullable CompletionStage<InstructionResponse> processBundleResponse;
  private volatile @Nullable String processBundleId = null;
  private StateDelegator.Registration deregisterStateHandler;

  private @Nullable String grpcReadTransformId = null;
  private String grpcReadTransformOutputName = null;

  public RegisterAndProcessBundleOperation(
      Supplier<String> idGenerator,
      InstructionRequestHandler instructionRequestHandler,
      StateDelegator beamFnStateDelegator,
      RegisterRequest registerRequest,
      Map<String, DataflowOperationContext> ptransformIdToOperationContext,
      Map<String, DataflowStepContext> ptransformIdToSystemStepContext,
      Map<String, SideInputReader> ptransformIdToSideInputReader,
      Table<String, String, PCollectionView<?>> ptransformIdToSideInputIdToPCollectionView,
      OperationContext context) {
    super(EMPTY_RECEIVERS, context);
    this.idGenerator = idGenerator;
    this.instructionRequestHandler = instructionRequestHandler;
    this.beamFnStateDelegator = beamFnStateDelegator;
    this.registerRequest = registerRequest;
    this.ptransformIdToSideInputReader = ptransformIdToSideInputReader;
    this.ptransformIdToSideInputIdToPCollectionView = ptransformIdToSideInputIdToPCollectionView;
    ImmutableMap.Builder<String, DataflowStepContext> userStepContextsMap = ImmutableMap.builder();
    for (Map.Entry<String, DataflowStepContext> entry :
        ptransformIdToSystemStepContext.entrySet()) {
      userStepContextsMap.put(entry.getKey(), entry.getValue().namespacedToUser());
    }
    this.ptransformIdToUserStepContext = userStepContextsMap.build();
    this.userStateData = new ConcurrentHashMap<>();

    checkState(
        registerRequest.getProcessBundleDescriptorCount() == 1,
        "Only one bundle registration at a time currently supported.");
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Process bundle descriptor {}", toDot(registerRequest.getProcessBundleDescriptor(0)));
    }
    for (Map.Entry<String, RunnerApi.PTransform> pTransform :
        registerRequest.getProcessBundleDescriptor(0).getTransformsMap().entrySet()) {
      if (pTransform.getValue().getSpec().getUrn().equals(RemoteGrpcPortRead.URN)) {
        if (grpcReadTransformId != null) {
          // TODO: Handle the case of more than one input.
          grpcReadTransformId = null;
          grpcReadTransformOutputName = null;
          break;
        }
        grpcReadTransformId = pTransform.getKey();
        grpcReadTransformOutputName =
            Iterables.getOnlyElement(pTransform.getValue().getOutputsMap().keySet());
      }
    }
  }

  /** Generates a dot description of the process bundle descriptor. */
  private static String toDot(ProcessBundleDescriptor processBundleDescriptor) {
    StringBuilder builder = new StringBuilder();
    builder.append("digraph network {\n");
    Map<String, String> nodeName = Maps.newHashMap();
    processBundleDescriptor
        .getPcollectionsMap()
        .forEach((key, node) -> nodeName.put("pc " + key, "n" + nodeName.size()));
    processBundleDescriptor
        .getTransformsMap()
        .forEach((key, node) -> nodeName.put("pt " + key, "n" + nodeName.size()));
    for (Entry<String, RunnerApi.PCollection> nodeEntry :
        processBundleDescriptor.getPcollectionsMap().entrySet()) {
      builder.append(
          String.format(
              "  %s [fontname=\"Courier New\" label=\"%s\"];\n",
              nodeName.get("pc " + nodeEntry.getKey()),
              escapeDot(nodeEntry.getKey() + ": " + nodeEntry.getValue().getUniqueName())));
    }
    for (Entry<String, RunnerApi.PTransform> nodeEntry :
        processBundleDescriptor.getTransformsMap().entrySet()) {
      builder.append(
          String.format(
              "  %s [fontname=\"Courier New\" label=\"%s\"];\n",
              nodeName.get("pt " + nodeEntry.getKey()),
              escapeDot(
                  nodeEntry.getKey()
                      + ": "
                      + nodeEntry.getValue().getSpec().getUrn()
                      + " "
                      + nodeEntry.getValue().getUniqueName())));
      for (Entry<String, String> inputEntry : nodeEntry.getValue().getInputsMap().entrySet()) {
        builder.append(
            String.format(
                "  %s -> %s [fontname=\"Courier New\" label=\"%s\"];\n",
                nodeName.get("pc " + inputEntry.getValue()),
                nodeName.get("pt " + nodeEntry.getKey()),
                escapeDot(inputEntry.getKey())));
      }
      for (Entry<String, String> outputEntry : nodeEntry.getValue().getOutputsMap().entrySet()) {
        builder.append(
            String.format(
                "  %s -> %s [fontname=\"Courier New\" label=\"%s\"];\n",
                nodeName.get("pt " + nodeEntry.getKey()),
                nodeName.get("pc " + outputEntry.getValue()),
                escapeDot(outputEntry.getKey())));
      }
    }
    builder.append("}");
    return builder.toString();
  }

  private static String escapeDot(String s) {
    return s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        // http://www.graphviz.org/doc/info/attrs.html#k:escString
        // The escape sequences "\n", "\l" and "\r" divide the label into lines, centered,
        // left-justified, and right-justified, respectively.
        .replace("\n", "\\l");
  }

  /**
   * Returns an id for the current bundle being processed.
   *
   * <p><b>Note</b>: This operation could be used across multiple bundles, so a unique id is
   * generated for every bundle. {@link Operation Operations} accessing the bundle id should only
   * call this once per bundle and cache the id in the {@link Operation#start()} method and clear it
   * in the {@link Operation#finish()} method.
   */
  public synchronized String getProcessBundleInstructionId() {
    if (processBundleId == null) {
      processBundleId = idGenerator.get();
    }
    return processBundleId;
  }

  @Override
  public void start() throws Exception {
    try (Closeable scope = context.enterStart()) {
      super.start();

      // Only register once by using the presence of the future as a signal.
      if (registerFuture == null) {
        InstructionRequest request =
            InstructionRequest.newBuilder()
                .setInstructionId(idGenerator.get())
                .setRegister(registerRequest)
                .build();
        registerFuture = instructionRequestHandler.handle(request);
        getRegisterResponse(registerFuture);
      }

      checkState(
          registerRequest.getProcessBundleDescriptorCount() == 1,
          "Only one bundle registration at a time currently supported.");
      InstructionRequest processBundleRequest =
          InstructionRequest.newBuilder()
              .setInstructionId(getProcessBundleInstructionId())
              .setProcessBundle(
                  ProcessBundleRequest.newBuilder()
                      .setProcessBundleDescriptorReference(
                          registerRequest.getProcessBundleDescriptor(0).getId()))
              .build();

      deregisterStateHandler =
          beamFnStateDelegator.registerForProcessBundleInstructionId(
              getProcessBundleInstructionId(), this::delegateByStateKeyType);
      processBundleResponse = instructionRequestHandler.handle(processBundleRequest);
    }
  }

  @Override
  public void finish() throws Exception {
    // TODO: Once we have access to windowing strategy via the ParDoPayload, add support to garbage
    // collect any user state set. Also add support for consuming those garbage collection timers.

    try (Closeable scope = context.enterFinish()) {
      // Await completion or failure
      MoreFutures.get(getProcessBundleResponse(processBundleResponse));
      deregisterStateHandler.deregister();
      userStateData.clear();
      processBundleId = null;
      super.finish();
    }
  }

  @Override
  public void abort() throws Exception {
    try (Closeable scope = context.enterAbort()) {
      deregisterStateHandler.abort();
      cancelIfNotNull(registerFuture);
      cancelIfNotNull(processBundleResponse);
      super.abort();
    }
  }

  // todomigryz: Extract common metrics processing code from getProcessBundleProgress and
  // getFinalMetrics.

  public Map<String, DataflowStepContext> getPtransformIdToUserStepContext() {
    return ptransformIdToUserStepContext;
  }

  /**
   * Returns the compound metrics recorded, by issuing a request to the SDK harness.
   *
   * <p>This includes key progress indicators in {@link BeamFnApi.Metrics.PTransform.Measured} as
   * well as user-defined metrics in {@link BeamFnApi.Metrics.User}.
   *
   * <p>Use {@link #getInputElementsConsumed(BeamFnApi.Metrics)} on the future value to extract the
   * elements consumed from the upstream read operation.
   *
   * <p>May be called at any time, including before start() and after finish().
   *
   * @throws InterruptedException
   * @throws ExecutionException
   */
  public CompletionStage<BeamFnApi.ProcessBundleProgressResponse> getProcessBundleProgress()
      throws InterruptedException, ExecutionException {
    // processBundleId may be reset if this bundle finishes asynchronously.
    String processBundleId = this.processBundleId;

    if (processBundleId == null) {
      return CompletableFuture.completedFuture(
          BeamFnApi.ProcessBundleProgressResponse.getDefaultInstance());
    }

    InstructionRequest processBundleRequest =
        InstructionRequest.newBuilder()
            .setInstructionId(idGenerator.get())
            .setProcessBundleProgress(
                ProcessBundleProgressRequest.newBuilder().setInstructionReference(processBundleId))
            .build();

    return instructionRequestHandler
        .handle(processBundleRequest)
        .thenApply(
            response -> {
              if (!response.getError().isEmpty()) {
                throw new IllegalStateException(response.getError());
              }
              return response.getProcessBundleProgress();
            });
  }

  /** Returns the final metrics returned by the SDK harness when it completes the bundle. */
  public CompletionStage<BeamFnApi.Metrics> getFinalMetrics() {
    return getProcessBundleResponse(processBundleResponse)
        .thenApply(response -> response.getMetrics());
  }

  public CompletionStage<List<MonitoringInfo>> getFinalMonitoringInfos() {
    return getProcessBundleResponse(processBundleResponse)
        .thenApply(response -> response.getMonitoringInfosList());
  }

  public boolean hasFailed() throws ExecutionException, InterruptedException {
    if (processBundleResponse != null && processBundleResponse.toCompletableFuture().isDone()) {
      return !processBundleResponse.toCompletableFuture().get().getError().isEmpty();
    } else {
      // At the very least, we don't know that this has failed yet.
      return false;
    }
  }

  /** Returns the number of input elements consumed by the gRPC read, if known, otherwise 0. */
  double getInputElementsConsumed(BeamFnApi.Metrics metrics) {
    return metrics
        .getPtransformsOrDefault(
            grpcReadTransformId, BeamFnApi.Metrics.PTransform.getDefaultInstance())
        .getProcessedElements()
        .getMeasured()
        .getOutputElementCountsOrDefault(grpcReadTransformOutputName, 0);
  }

  private CompletionStage<BeamFnApi.StateResponse.Builder> delegateByStateKeyType(
      StateRequest stateRequest) {
    switch (stateRequest.getStateKey().getTypeCase()) {
      case BAG_USER_STATE:
        return handleBagUserState(stateRequest);
      case MULTIMAP_SIDE_INPUT:
        return handleMultimapSideInput(stateRequest);
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Dataflow does not handle StateRequests of type %s",
                stateRequest.getStateKey().getTypeCase()));
    }
  }

  private CompletionStage<BeamFnApi.StateResponse.Builder> handleMultimapSideInput(
      StateRequest stateRequest) {
    checkState(
        stateRequest.getRequestCase() == RequestCase.GET,
        String.format(
            "MultimapSideInput state requests only support '%s' requests, received '%s'",
            RequestCase.GET, stateRequest.getRequestCase()));

    StateKey.MultimapSideInput multimapSideInputStateKey =
        stateRequest.getStateKey().getMultimapSideInput();

    SideInputReader sideInputReader =
        ptransformIdToSideInputReader.get(multimapSideInputStateKey.getPtransformId());
    checkState(
        sideInputReader != null,
        String.format("Unknown PTransform '%s'", multimapSideInputStateKey.getPtransformId()));

    PCollectionView<Materializations.MultimapView<Object, Object>> view =
        (PCollectionView<Materializations.MultimapView<Object, Object>>)
            ptransformIdToSideInputIdToPCollectionView.get(
                multimapSideInputStateKey.getPtransformId(),
                multimapSideInputStateKey.getSideInputId());
    checkState(
        view != null,
        String.format(
            "Unknown side input '%s' on PTransform '%s'",
            multimapSideInputStateKey.getSideInputId(),
            multimapSideInputStateKey.getPtransformId()));
    checkState(
        Materializations.MULTIMAP_MATERIALIZATION_URN.equals(
            view.getViewFn().getMaterialization().getUrn()),
        String.format(
            "Unknown materialization for side input '%s' on PTransform '%s' with urn '%s'",
            multimapSideInputStateKey.getSideInputId(),
            multimapSideInputStateKey.getPtransformId(),
            view.getViewFn().getMaterialization().getUrn()));
    checkState(
        view.getCoderInternal() instanceof KvCoder,
        String.format(
            "Materialization of side input '%s' on PTransform '%s' expects %s but received %s.",
            multimapSideInputStateKey.getSideInputId(),
            multimapSideInputStateKey.getPtransformId(),
            KvCoder.class.getSimpleName(),
            view.getCoderInternal().getClass().getSimpleName()));
    Coder<Object> keyCoder = ((KvCoder) view.getCoderInternal()).getKeyCoder();
    Coder<Object> valueCoder = ((KvCoder) view.getCoderInternal()).getValueCoder();

    BoundedWindow window;
    try {
      // TODO: Use EncodedWindow instead of decoding the window.
      window =
          view.getWindowingStrategyInternal()
              .getWindowFn()
              .windowCoder()
              .decode(multimapSideInputStateKey.getWindow().newInput());
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to decode window for side input '%s' on PTransform '%s'.",
              multimapSideInputStateKey.getSideInputId(),
              multimapSideInputStateKey.getPtransformId()),
          e);
    }

    Object userKey;
    try {
      // TODO: Use the encoded representation of the key.
      userKey = keyCoder.decode(multimapSideInputStateKey.getKey().newInput());
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to decode user key for side input '%s' on PTransform '%s'.",
              multimapSideInputStateKey.getSideInputId(),
              multimapSideInputStateKey.getPtransformId()),
          e);
    }

    Materializations.MultimapView<Object, Object> sideInput = sideInputReader.get(view, window);
    Iterable<Object> values = sideInput.get(userKey);
    try {
      // TODO: Chunk the requests and use a continuation key to support side input values
      // that are larger then 2 GiBs.
      // TODO: Use the raw value so we don't go through a decode/encode cycle for no reason.
      return CompletableFuture.completedFuture(
          StateResponse.newBuilder()
              .setGet(StateGetResponse.newBuilder().setData(encodeAndConcat(values, valueCoder))));
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to encode values for side input '%s' on PTransform '%s'.",
              multimapSideInputStateKey.getSideInputId(),
              multimapSideInputStateKey.getPtransformId()),
          e);
    }
  }

  private CompletionStage<BeamFnApi.StateResponse.Builder> handleBagUserState(
      StateRequest stateRequest) {
    StateKey.BagUserState bagUserStateKey = stateRequest.getStateKey().getBagUserState();
    DataflowStepContext userStepContext =
        ptransformIdToUserStepContext.get(bagUserStateKey.getPtransformId());
    checkState(
        userStepContext != null,
        String.format("Unknown PTransform id '%s'", bagUserStateKey.getPtransformId()));
    // TODO: We should not be required to hold onto a pointer to the bag states for the
    // user. InMemoryStateInternals assumes that the Java garbage collector does the clean-up work
    // but instead StateInternals should hold its own references and write out any data and
    // clear references when the MapTask within Dataflow completes like how WindmillStateInternals
    // works.
    BagState<ByteString> state =
        userStateData.computeIfAbsent(
            stateRequest.getStateKey(),
            unused ->
                userStepContext
                    .stateInternals()
                    .state(
                        // TODO: Once we have access to the ParDoPayload, use its windowing strategy
                        // to decode the window for the well known window types. Longer term we need
                        // to swap
                        // to use the encoded version and not rely on needing to decode the entire
                        // window.
                        StateNamespaces.window(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE),
                        StateTags.bag(bagUserStateKey.getUserStateId(), ByteStringCoder.of())));
    switch (stateRequest.getRequestCase()) {
      case GET:
        return CompletableFuture.completedFuture(
            StateResponse.newBuilder()
                .setGet(StateGetResponse.newBuilder().setData(concat(state.read()))));
      case APPEND:
        state.add(stateRequest.getAppend().getData());
        return CompletableFuture.completedFuture(
            StateResponse.newBuilder().setAppend(StateAppendResponse.getDefaultInstance()));
      case CLEAR:
        state.clear();
        return CompletableFuture.completedFuture(
            StateResponse.newBuilder().setClear(StateClearResponse.getDefaultInstance()));
      default:
        throw new IllegalArgumentException(
            String.format("Unknown request type %s", stateRequest.getRequestCase()));
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }

  private static CompletionStage<BeamFnApi.InstructionResponse> throwIfFailure(
      CompletionStage<InstructionResponse> responseFuture) {
    return responseFuture.thenApply(
        response -> {
          if (!response.getError().isEmpty()) {
            throw new IllegalStateException(
                String.format(
                    "Client failed to process %s with error [%s].",
                    response.getInstructionId(), response.getError()));
          }
          return response;
        });
  }

  private static CompletionStage<BeamFnApi.ProcessBundleResponse> getProcessBundleResponse(
      CompletionStage<InstructionResponse> responseFuture) {
    return throwIfFailure(responseFuture)
        .thenApply(
            response -> {
              switch (response.getResponseCase()) {
                case PROCESS_BUNDLE:
                  return response.getProcessBundle();
                default:
                  throw new IllegalStateException(
                      String.format(
                          "SDK harness returned wrong kind of response to ProcessBundleRequest: %s",
                          TextFormat.printToString(response)));
              }
            });
  }

  private static CompletionStage<BeamFnApi.RegisterResponse> getRegisterResponse(
      CompletionStage<InstructionResponse> responseFuture)
      throws ExecutionException, InterruptedException {
    return throwIfFailure(responseFuture)
        .thenApply(
            response -> {
              switch (response.getResponseCase()) {
                case REGISTER:
                  return response.getRegister();
                default:
                  throw new IllegalStateException(
                      String.format(
                          "SDK harness returned wrong kind of response to RegisterRequest: %s",
                          TextFormat.printToString(response)));
              }
            });
  }

  private static void cancelIfNotNull(CompletionStage<?> future) {
    if (future != null) {
      // TODO: add cancel(boolean) to MoreFutures
      future.toCompletableFuture().cancel(true);
    }
  }

  private ByteString concat(Iterable<ByteString> values) {
    ByteString rval = ByteString.EMPTY;
    if (values != null) {
      for (ByteString value : values) {
        rval = rval.concat(value);
      }
    }
    return rval;
  }

  static ByteString encodeAndConcat(Iterable<Object> values, Coder valueCoder) throws IOException {
    ByteString.Output out = ByteString.newOutput();
    if (values != null) {
      for (Object value : values) {
        int size = out.size();
        valueCoder.encode(value, out);
        // Pad empty values by one byte as per the Beam Fn data transfer specification.
        if (size == out.size()) {
          out.write(0);
        }
      }
    }
    return out.toByteString();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("processBundleId", processBundleId)
        .add("processBundleDescriptors", registerRequest.getProcessBundleDescriptorList())
        .toString();
  }
}
