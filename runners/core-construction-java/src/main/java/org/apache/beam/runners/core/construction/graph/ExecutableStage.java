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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;

import com.google.common.base.MoreObjects;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.Environments;
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
public abstract class ExecutableStage {
  /**
   * The URN identifying an {@link ExecutableStage} that has been converted to a {@link PTransform}.
   */
  public static final String URN = "beam:runner:executable_stage:v1";

  /**
   * Returns the {@link Environment} this stage executes in.
   *
   * <p>An {@link ExecutableStage} consists of {@link PTransform PTransforms} which can all be
   * executed within a single {@link Environment}. The assumption made here is that
   * runner-implemented transforms will be associated with these subgraphs by the overall graph
   * topology, which will be handled by runners by performing already-required element routing and
   * runner-side processing.
   */
  public abstract Environment getEnvironment();

  /**
   * Returns the root {@link PCollectionNode} of this {@link ExecutableStage}. This
   * {@link ExecutableStage} executes by reading elements from a Remote gRPC Read Node.
   */
  public abstract PCollectionNode getInputPCollection();

  /**
   * Returns the leaf {@link PCollectionNode PCollections} of this {@link ExecutableStage}.
   *
   * <p>All of these {@link PCollectionNode PCollections} are consumed by a {@link PTransformNode
   * PTransform} which is not contained within this executable stage, and must be materialized at
   * execution time by a Remote gRPC Write Transform.
   */
  public abstract Collection<PCollectionNode> getOutputPCollections();

  /**
   * Get the transforms that perform processing within this {@link ExecutableStage}.
   */
  public abstract Collection<PTransformNode> getTransforms();

  /**
   * Returns a composite {@link PTransform} which contains all of the {@link PTransform PTransforms}
   * fused into this {@link ExecutableStage} as {@link PTransform#getSubtransformsList()
   * subtransforms} .
   *
   *
   * {@link PCollectionNode} returned by {@link #getInputPCollection()} and the output {@link
   * PCollection PCollections} will be the {@link PCollectionNode PCollections} returned by {@link
   * #getOutputPCollections()}.
   */
  public final PTransform toPTransform() {
    PTransform.Builder pt = PTransform.newBuilder();
    pt.putInputs("input", getInputPCollection().getId());
    int i = 0;
    for (PCollectionNode materializedPCollection : getOutputPCollections()) {
      pt.putOutputs(String.format("materialized_%s", i), materializedPCollection.getId());
      i++;
    }
    for (PTransformNode fusedTransform : getTransforms()) {
      pt.addSubtransforms(fusedTransform.getId());
    }
    pt.setSpec(FunctionSpec.newBuilder().setUrn(ExecutableStage.URN));
    return pt.build();
  }

  @Override
  public final boolean equals(Object o) {
    if (!(o instanceof ExecutableStage)) {
      return false;
    }
    ExecutableStage that = (ExecutableStage) o;
    return Objects.equals(this.getEnvironment(), that.getEnvironment())
        && Objects.equals(this.getInputPCollection(), that.getInputPCollection())
        && Objects.equals(this.getOutputPCollections(), that.getOutputPCollections())
        && Objects.equals(this.getTransforms(), that.getTransforms());
  }

  @Override
  public final int hashCode() {
    return Objects.hash(
        getEnvironment(), getInputPCollection(), getOutputPCollections(), getTransforms());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(GreedilyFusedExecutableStage.class)
        .add("inputPCollection", getInputPCollection().getId())
        .add(
            "outputPCollections",
            getOutputPCollections()
                .stream()
                .map(PCollectionNode::getId)
                .collect(Collectors.toList()))
        .add(
            "transforms",
            getTransforms().stream().map(PTransformNode::getId).collect(Collectors.toList()))
        .toString();
  }

  /**
   * Return an {@link ExecutableStage} constructed from the provided {@link PTransform}
   * representation.
   */
  public static ExecutableStage fromPTransform(PTransform ptransform, Components components) {
    checkArgument(ptransform.getSpec().getUrn().equals(URN));
    // It may be better to put this in an explicit Payload if other metadata becomes required
    Optional<Environment> environment =
        Environments.getEnvironment(
            ptransform.getSubtransforms(0), components);
    checkArgument(
        environment.isPresent(),
        "%s with no %s",
        ExecutableStage.class.getSimpleName(),
        Environment.class.getSimpleName());
    String inputId = getOnlyElement(ptransform.getInputsMap().values());
    PCollectionNode inputNode =
        PipelineNode.pCollection(inputId, components.getPcollectionsOrThrow(inputId));
    Collection<PCollectionNode> outputNodes =
        ptransform
            .getOutputsMap()
            .values()
            .stream()
            .map(id -> PipelineNode.pCollection(id, components.getPcollectionsOrThrow(id)))
            .collect(Collectors.toSet());
    Collection<PTransformNode> transformNodes =
        ptransform
            .getSubtransformsList()
            .stream()
            .map(id -> PipelineNode.pTransform(id, components.getTransformsOrThrow(id)))
            .collect(Collectors.toSet());
    return ImmutableExecutableStage.of(environment.get(), inputNode, transformNodes, outputNodes);
  }
}
