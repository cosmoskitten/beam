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
package org.apache.beam.runners.dataflow.worker.graph;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonGenerator;
import com.google.api.client.util.Charsets;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.auto.value.AutoValue;
import com.google.common.base.MoreObjects;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Operation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.Transport;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;

/** Container class for different types of network nodes. All nodes only have reference equality. */
public class Nodes {
  /** Base class for network nodes. All nodes only have reference equality. */
  public abstract static class Node {
    @Override
    public final boolean equals(Object obj) {
      return this == obj;
    }

    @Override
    public final int hashCode() {
      return super.hashCode();
    }
  }

  private static String toStringWithTrimmedLiterals(GenericJson json) {
    try {
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      final JsonGenerator baseGenerator =
          MoreObjects.firstNonNull(json.getFactory(), Transport.getJsonFactory())
              .createJsonGenerator(byteStream, Charsets.UTF_8);
      JsonGenerator generator =
          new JsonGenerator() {
            @Override
            public void writeString(String value) throws IOException {
              if (value.length() > 100) {
                baseGenerator.writeString(value.substring(0, 100) + "...");
              } else {
                baseGenerator.writeString(value);
              }
            }

            @Override
            public JsonFactory getFactory() {
              return baseGenerator.getFactory();
            }

            @Override
            public void flush() throws IOException {
              baseGenerator.flush();
            }

            @Override
            public void close() throws IOException {
              baseGenerator.close();
            }

            @Override
            public void writeStartArray() throws IOException {
              baseGenerator.writeStartArray();
            }

            @Override
            public void writeEndArray() throws IOException {
              baseGenerator.writeEndArray();
            }

            @Override
            public void writeStartObject() throws IOException {
              baseGenerator.writeStartObject();
            }

            @Override
            public void writeEndObject() throws IOException {
              baseGenerator.writeEndObject();
            }

            @Override
            public void writeFieldName(String name) throws IOException {
              baseGenerator.writeFieldName(name);
            }

            @Override
            public void writeNull() throws IOException {
              baseGenerator.writeNull();
            }

            @Override
            public void writeBoolean(boolean state) throws IOException {
              baseGenerator.writeBoolean(state);
            }

            @Override
            public void writeNumber(int v) throws IOException {
              baseGenerator.writeNumber(v);
            }

            @Override
            public void writeNumber(long v) throws IOException {
              baseGenerator.writeNumber(v);
            }

            @Override
            public void writeNumber(BigInteger v) throws IOException {
              baseGenerator.writeNumber(v);
            }

            @Override
            public void writeNumber(float v) throws IOException {
              baseGenerator.writeNumber(v);
            }

            @Override
            public void writeNumber(double v) throws IOException {
              baseGenerator.writeNumber(v);
            }

            @Override
            public void writeNumber(BigDecimal v) throws IOException {
              baseGenerator.writeNumber(v);
            }

            @Override
            public void writeNumber(String encodedValue) throws IOException {
              baseGenerator.writeNumber(encodedValue);
            }

            @Override
            public void enablePrettyPrint() throws IOException {
              baseGenerator.enablePrettyPrint();
            }
          };
      generator.enablePrettyPrint();
      generator.serialize(json);
      generator.flush();
      return byteStream.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Property indicating on which harness a ParallelInstructionNode will execute. */
  public enum ExecutionLocation {
    UNKNOWN, // Indicates location has not yet been decided.
    SDK_HARNESS,
    RUNNER_HARNESS,
    AMBIGUOUS // Node can execute in either or both harnesses.
  }

  /** A node that stores {@link ParallelInstruction}s. */
  @AutoValue
  public abstract static class ParallelInstructionNode extends Node {
    public static ParallelInstructionNode create(
        ParallelInstruction parallelInstruction, ExecutionLocation executionLocation) {
      checkNotNull(parallelInstruction);
      checkNotNull(executionLocation);
      return new AutoValue_Nodes_ParallelInstructionNode(parallelInstruction, executionLocation);
    }

    public abstract ParallelInstruction getParallelInstruction();

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("parallelInstruction", toStringWithTrimmedLiterals(getParallelInstruction()))
          .add("executionLocation", getExecutionLocation().toString())
          .toString();
    }

    public abstract ExecutionLocation getExecutionLocation();
  }

  /** A node that stores {@link InstructionOutput}s. */
  @AutoValue
  public abstract static class InstructionOutputNode extends Node {
    public static InstructionOutputNode create(InstructionOutput instructionOutput) {
      checkNotNull(instructionOutput);
      return new AutoValue_Nodes_InstructionOutputNode(instructionOutput);
    }

    public abstract InstructionOutput getInstructionOutput();

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("instructionOutput", toStringWithTrimmedLiterals(getInstructionOutput()))
          .toString();
    }
  }

  /** A node that stores {@link OutputReceiver}s. */
  @AutoValue
  public abstract static class OutputReceiverNode extends Node {
    public static OutputReceiverNode create(OutputReceiver outputReceiver, Coder<?> coder) {
      checkNotNull(outputReceiver);
      return new AutoValue_Nodes_OutputReceiverNode(outputReceiver, coder);
    }

    public abstract OutputReceiver getOutputReceiver();

    public abstract Coder<?> getCoder();
  }

  /** A node that stores {@link Operation}s. */
  @AutoValue
  public abstract static class OperationNode extends Node {
    public static OperationNode create(Operation operation) {
      checkNotNull(operation);
      return new AutoValue_Nodes_OperationNode(operation);
    }

    public abstract Operation getOperation();
  }

  /** A node that stores {@link org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort}s. */
  @AutoValue
  public abstract static class RemoteGrpcPortNode extends Node {
    public static RemoteGrpcPortNode create(
        BeamFnApi.RemoteGrpcPort port,
        String primitiveTransformId,
        String functionSpecId,
        String inputId,
        String outputId) {
      checkNotNull(port);
      return new AutoValue_Nodes_RemoteGrpcPortNode(
          port, primitiveTransformId, functionSpecId, inputId, outputId);
    }

    public abstract BeamFnApi.RemoteGrpcPort getRemoteGrpcPort();

    public abstract String getPrimitiveTransformId();

    public abstract String getFunctionSpecId();

    public abstract String getInputId();

    public abstract String getOutputId();
  }

  /** A node that stores {@link org.apache.beam.model.fnexecution.v1.BeamFnApi.RegisterRequest}s. */
  @AutoValue
  public abstract static class RegisterRequestNode extends Node {
    public static RegisterRequestNode create(
        BeamFnApi.RegisterRequest request,
        Map<String, NameContext> ptransformIdToPartialNameContextMap,
        Map<String, Iterable<SideInputInfo>> ptransformIdToSideInputInfoMap,
        Map<String, Iterable<PCollectionView<?>>> pTransformIdToPCollectionViewMap) {
      checkNotNull(request);
      checkNotNull(ptransformIdToPartialNameContextMap);
      return new AutoValue_Nodes_RegisterRequestNode(
          request,
          ptransformIdToPartialNameContextMap,
          ptransformIdToSideInputInfoMap,
          pTransformIdToPCollectionViewMap);
    }

    public abstract BeamFnApi.RegisterRequest getRegisterRequest();

    public abstract Map<String, NameContext> getPTransformIdToPartialNameContextMap();

    public abstract Map<String, Iterable<SideInputInfo>> getPTransformIdToSideInputInfoMap();

    public abstract Map<String, Iterable<PCollectionView<?>>> getPTransformIdToPCollectionViewMap();

    @Override
    public String toString() {
      // The request may be very large.
      return "RegisterRequestNode";
    }
  }

  /** A node that stores {@link org.apache.beam.runners.core.construction.graph.ExecutableStage}. */
  @AutoValue
  public abstract static class ExecutableStageNode extends Node {
    public static ExecutableStageNode create(
        ExecutableStage executableStage,
        Map<String, NameContext> ptransformIdToPartialNameContextMap) {
      checkNotNull(executableStage);
      checkNotNull(ptransformIdToPartialNameContextMap);
      return new AutoValue_Nodes_ExecutableStageNode(
          executableStage, ptransformIdToPartialNameContextMap);
    }

    public abstract ExecutableStage getExecutableStage();

    public abstract Map<String, NameContext> getPTransformIdToPartialNameContextMap();

    @Override
    public String toString() {
      // The request may be very large.
      return "ExecutableStageNode";
    }
  }

  /**
   * A node in the graph responsible for fetching side inputs that are ready and also filtering
   * elements which are blocked after asking the SDK harness to perform any window mapping.
   *
   * <p>Note that this should only be used within streaming pipelines.
   */
  @AutoValue
  public abstract static class FetchAndFilterStreamingSideInputsNode extends Node {
    public static FetchAndFilterStreamingSideInputsNode create(
        WindowingStrategy<?, ?> windowingStrategy,
        Map<PCollectionView<?>, RunnerApi.SdkFunctionSpec> pCollectionViewsToWindowMappingFns,
        NameContext nameContext) {
      return new AutoValue_Nodes_FetchAndFilterStreamingSideInputsNode(
          windowingStrategy, pCollectionViewsToWindowMappingFns, nameContext);
    }

    public abstract WindowingStrategy<?, ?> getWindowingStrategy();

    public abstract Map<PCollectionView<?>, RunnerApi.SdkFunctionSpec>
        getPCollectionViewsToWindowMappingFns();

    public abstract NameContext getNameContext();
  }

  // Hide visibility to prevent instantiation
  private Nodes() {}
}
