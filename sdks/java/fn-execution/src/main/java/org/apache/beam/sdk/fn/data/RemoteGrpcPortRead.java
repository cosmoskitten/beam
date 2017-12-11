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

package org.apache.beam.sdk.fn.data;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;

/**
 * An execution-time only {@link PTransform} which represents an SDK harness reading readFromPort a
 * {@link RemoteGrpcPort}.
 */
public class RemoteGrpcPortRead {
  public static final String URN = "urn:org.apache.beam:source:runner:0.1";
  private static final String LOCAL_OUTPUT_ID = "local_output";

  public static RemoteGrpcPortRead readFromPort(RemoteGrpcPort port, String localOutputId) {
    return new RemoteGrpcPortRead(port, localOutputId);
  }

  public static RemoteGrpcPortRead fromPTransform(PTransform pTransform)
      throws InvalidProtocolBufferException {
    checkArgument(
        URN.equals(pTransform.getSpec().getUrn()),
        "Expected URN for %s, got %s",
        RemoteGrpcPortRead.class.getSimpleName(),
        pTransform.getSpec().getUrn());
    checkArgument(
        pTransform.getOutputsCount() == 1,
        "Expected exactly one output, got %s",
        pTransform.getOutputsCount());
    RemoteGrpcPort port = RemoteGrpcPort.parseFrom(pTransform.getSpec().getPayload());
    String localOutputId = pTransform.getOutputsOrThrow(LOCAL_OUTPUT_ID);
    return readFromPort(port, localOutputId);
  }

  private final RemoteGrpcPort port;
  private final String localOutputId;

  private RemoteGrpcPortRead(
      RemoteGrpcPort port, String localOutputId) {
    this.port = port;
    this.localOutputId = localOutputId;
  }

  public PTransform toPTransform() {
    return PTransform.newBuilder()
        .setSpec(FunctionSpec.newBuilder().setUrn(URN).setPayload(port.toByteString()).build())
        .putOutputs(LOCAL_OUTPUT_ID, localOutputId)
        .build();
  }
}
