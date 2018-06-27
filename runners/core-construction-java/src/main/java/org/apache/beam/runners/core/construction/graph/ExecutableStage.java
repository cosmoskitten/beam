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

package org.apache.beam.runners.core.construction.graph;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;

/**
 * A combination of PTransforms that can be executed within a single SDK harness.
 *
 * <p>Contains only the nodes that specify the processing to perform within the SDK harness, and
 * does not contain any runner-executed nodes.
 *
 * <p>Within a single {@link Pipeline}, {@link PTransform PTransforms} and {@link PCollection
 * PCollections} are permitted to appear in multiple executable stages. However, paths from a root
 * {@link PTransform} to any other {@link PTransform} within that set of stages must be unique.
 */
public interface ExecutableStage {
  /**
   * The URN identifying an {@link ExecutableStage} that has been converted to a {@link PTransform}.
   */
  String URN = "beam:runner:executable_stage:v1";

  /**
   * Return the {@link Components} required to execute this {@link ExecutableStage}.
   *
   * <p>This must contain all of the transforms returned by {@link #getTransforms()} and the closure
   * of all components that those {@link PTransformNode transforms} reference.
   */
  RunnerApi.Components getComponents();

  /**
   * Returns the {@link Environment} this stage executes in.
   *
   * <p>An {@link ExecutableStage} consists of {@link PTransform PTransforms} which can all be
   * executed within a single {@link Environment}. The assumption made here is that
   * runner-implemented transforms will be associated with these subgraphs by the overall graph
   * topology, which will be handled by runners by performing already-required element routing and
   * runner-side processing.
   */
  Environment getEnvironment();

  /**
   * Returns the root {@link PCollectionNode} of this {@link ExecutableStage}. This {@link
   * ExecutableStage} executes by reading elements from a Remote gRPC Read Node.
   */
  PCollectionNode getInputPCollection();

  /**
   * Returns the set of {@link PCollectionNode PCollections} that will be accessed by this {@link
   * ExecutableStage} as side inputs.
   */
  Collection<SideInputReference> getSideInputs();

  /**
   * Returns the leaf {@link PCollectionNode PCollections} of this {@link ExecutableStage}.
   *
   * <p>All of these {@link PCollectionNode PCollections} are consumed by a {@link PTransformNode
   * PTransform} which is not contained within this executable stage, and must be materialized at
   * execution time by a Remote gRPC Write Transform.
   */
  Collection<PCollectionNode> getOutputPCollections();

  /** Get the transforms that perform processing within this {@link ExecutableStage}. */
  Collection<PTransformNode> getTransforms();

  /**
   * Returns a composite {@link PTransform} which is equivalent to this {@link ExecutableStage} as
   * follows:
   *
   * <ul>
   *   <li>The {@link PTransform#getSubtransformsList()} is empty. This ensures that executable
   *       stages are treated as primitive transforms.
   *   <li>The only {@link PCollection} in the {@link PTransform#getInputsMap()} is the result of
   *       {@link #getInputPCollection()}.
   *   <li>The output {@link PCollection PCollections} in the values of {@link
   *       PTransform#getOutputsMap()} are the {@link PCollectionNode PCollections} returned by
   *       {@link #getOutputPCollections()}.
   *   <li>The {@link PTransform#getSpec()} contains an {@link ExecutableStagePayload} with inputs
   *       and outputs equal to the PTransform's inputs and outputs, and transforms equal to the
   *       result of {@link #getTransforms}.
   * </ul>
   *
   * <p>The executable stage can be reconstructed from the resulting {@link ExecutableStagePayload}
   * via {@link #fromPayload(ExecutableStagePayload)}.
   */
  default PTransform toPTransform(String uniqueName) {
    PTransform.Builder pt = PTransform.newBuilder().setUniqueName(uniqueName);
    ExecutableStagePayload.Builder payload = ExecutableStagePayload.newBuilder();

    payload.setEnvironment(getEnvironment());

    // Populate inputs and outputs of the stage payload and outer PTransform simultaneously.
    PCollectionNode input = getInputPCollection();
    pt.putInputs("input", getInputPCollection().getId());
    payload.setInput(input.getId());

    for (SideInputReference sideInput : getSideInputs()) {
      // Side inputs of the ExecutableStage itself can be uniquely identified by inner PTransform
      // name and local name.
      String outerLocalName = String.format("%s:%s", sideInput.transform(), sideInput.localName());
      pt.putInputs(outerLocalName, sideInput.collection().getId());
      payload.addSideInputs(
          SideInputId.newBuilder()
              .setTransformId(sideInput.transform().getId())
              .setLocalName(sideInput.localName())
              .build());
    }

    int outputIndex = 0;
    for (PCollectionNode output : getOutputPCollections()) {
      pt.putOutputs(String.format("materialized_%d", outputIndex), output.getId());
      payload.addOutputs(output.getId());
      outputIndex++;
    }

    // Inner PTransforms of this stage are hidden from the outer pipeline and only belong in the
    // stage payload.
    for (PTransformNode transform : getTransforms()) {
      payload.addTransforms(transform.getId());
    }
    payload.setComponents(
        getComponents()
            .toBuilder()
            .clearTransforms()
            .putAllTransforms(
                getTransforms()
                    .stream()
                    .collect(
                        Collectors.toMap(PTransformNode::getId, PTransformNode::getTransform))));

    pt.setSpec(
        FunctionSpec.newBuilder()
            .setUrn(ExecutableStage.URN)
            .setPayload(payload.build().toByteString())
            .build());
    return pt.build();
  }

  /**
   * Return an {@link ExecutableStage} constructed from the provided {@link FunctionSpec}
   * representation.
   *
   * <p>See {@link #toPTransform} for how the payload is constructed.
   *
   * <p>Note: The payload contains some information redundant with the {@link PTransform} it is the
   * payload of. The {@link ExecutableStagePayload} should be sufficiently rich to construct a
   * {@code ProcessBundleDescriptor} using only the payload.
   */
  static ExecutableStage fromPayload(ExecutableStagePayload payload) {
    Components components = payload.getComponents();
    Environment environment = payload.getEnvironment();
    PCollectionNode input =
        PipelineNode.pCollection(
            payload.getInput(), components.getPcollectionsOrThrow(payload.getInput()));
    List<SideInputReference> sideInputs =
        payload
            .getSideInputsList()
            .stream()
            .map(sideInputId -> SideInputReference.fromSideInputId(sideInputId, components))
            .collect(Collectors.toList());
    List<PTransformNode> transforms =
        payload
            .getTransformsList()
            .stream()
            .map(id -> PipelineNode.pTransform(id, components.getTransformsOrThrow(id)))
            .collect(Collectors.toList());
    List<PCollectionNode> outputs =
        payload
            .getOutputsList()
            .stream()
            .map(id -> PipelineNode.pCollection(id, components.getPcollectionsOrThrow(id)))
            .collect(Collectors.toList());
    return ImmutableExecutableStage.of(
        components, environment, input, sideInputs, transforms, outputs);
  }
}
