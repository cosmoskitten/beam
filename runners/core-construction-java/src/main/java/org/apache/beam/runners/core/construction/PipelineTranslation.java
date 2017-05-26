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

package org.apache.beam.runners.core.construction;

import com.google.auto.value.AutoValue;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.protobuf.Any;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.PTransformTranslation.RawPTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/** Utilities for going to/from Runner API pipelines. */
public class PipelineTranslation {

  public static RunnerApi.Pipeline translatePipeline(final Pipeline pipeline) {
    final SdkComponents components = SdkComponents.create();
    final Collection<String> rootIds = new HashSet<>();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          private final ListMultimap<Node, AppliedPTransform<?, ?, ?>> children =
              ArrayListMultimap.create();

          @Override
          public void leaveCompositeTransform(Node node) {
            if (node.isRootNode()) {
              for (AppliedPTransform<?, ?, ?> pipelineRoot : children.get(node)) {
                rootIds.add(components.getExistingPTransformId(pipelineRoot));
              }
            } else {
              children.put(node.getEnclosingNode(), node.toAppliedPTransform(pipeline));
              try {
                components.registerPTransform(
                    node.toAppliedPTransform(pipeline), children.get(node));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          }

          @Override
          public void visitPrimitiveTransform(Node node) {
            children.put(node.getEnclosingNode(), node.toAppliedPTransform(pipeline));
            try {
              components.registerPTransform(
                  node.toAppliedPTransform(pipeline),
                  Collections.<AppliedPTransform<?, ?, ?>>emptyList());
            } catch (IOException e) {
              throw new IllegalStateException(e);
            }
          }
        });
    // TODO: Display Data
    return RunnerApi.Pipeline.newBuilder()
        .setComponents(components.toComponents())
        .addAllRootTransformIds(rootIds)
        .build();
  }

  public static Pipeline rehydratePipeline(final RunnerApi.Pipeline pipelineProto)
      throws IOException {
    TransformHierarchy transforms = new TransformHierarchy();
    Pipeline pipeline = Pipeline.forTransformHierarchy(transforms, PipelineOptionsFactory.create());

    Map<String, PCollection<?>> visitedPCollections = new HashMap<>();

    for (String rootId : pipelineProto.getRootTransformIdsList()) {
      addRehydratedTransform(
          transforms,
          pipelineProto.getComponents().getTransformsOrThrow(rootId),
          pipelineProto.getComponents(),
          pipeline,
          visitedPCollections);
    }

    return pipeline;
  }

  private static void addRehydratedTransform(
      TransformHierarchy transforms,
      RunnerApi.PTransform transformProto,
      RunnerApi.Components components,
      Pipeline pipeline,
      Map<String, PCollection<?>> visitedPCollections)
      throws IOException {

    Map<TupleTag<?>, PValue> rehydratedInputs = new HashMap<>();
    for (Map.Entry<String, String> inputEntry : transformProto.getInputsMap().entrySet()) {
      rehydratedInputs.put(
          new TupleTag<>(inputEntry.getKey()),
          rehydratePCollection(inputEntry.getValue(), components, pipeline, visitedPCollections));
    }

    Map<TupleTag<?>, PValue> rehydratedOutputs = new HashMap<>();
    for (Map.Entry<String, String> outputEntry : transformProto.getOutputsMap().entrySet()) {
      rehydratedOutputs.put(
          new TupleTag<>(outputEntry.getKey()),
          rehydratePCollection(outputEntry.getValue(), components, pipeline, visitedPCollections));
    }

    RunnerApi.FunctionSpec transformSpec = transformProto.getSpec();

    // By default, no "additional" inputs, since that is an SDK-specific thing.
    // Only ParDo really separates main from side inputs
    Map<TupleTag<?>, PValue> additionalInputs = Collections.emptyMap();

    // TODO: move this ownership into the ParDoTranslator
    if (transformSpec.getUrn().equals(PTransformTranslation.PAR_DO_TRANSFORM_URN)) {
      RunnerApi.ParDoPayload payload =
          transformSpec.getParameter().unpack(RunnerApi.ParDoPayload.class);

      List<PCollectionView<?>> views = new ArrayList<>();
      for (Map.Entry<String, RunnerApi.SideInput> sideInput :
          payload.getSideInputsMap().entrySet()) {
        views.add(
            ParDoTranslation.viewFromProto(
                pipeline, sideInput.getValue(), sideInput.getKey(), transformProto, components));
      }
      additionalInputs = PCollectionViews.toAdditionalInputs(views);
    }

    RehydratedPTransform transform =
        RehydratedPTransform.of(
            transformSpec.getUrn(),
            transformSpec.getParameter(),
            additionalInputs);

    // HACK: A primitive is something with outputs that are not in its input
    if (transformProto.getSubtransformsCount() > 0
        || transformProto
            .getInputsMap()
            .values()
            .containsAll(transformProto.getOutputsMap().values())) {
      transforms.pushFinalizedNode(
          transformProto.getUniqueName(),
          rehydratedInputs,
          transform,
          rehydratedOutputs);

      for (String childTransformId : transformProto.getSubtransformsList()) {
        addRehydratedTransform(
            transforms,
            components.getTransformsOrThrow(childTransformId),
            components,
            pipeline,
            visitedPCollections);
      }

      transforms.popNode();
    } else {
      transforms.addFinalizedPrimitiveNode(
          transformProto.getUniqueName(),
          rehydratedInputs,
          transform,
          rehydratedOutputs);
    }
  }

  private static PCollection<?> rehydratePCollection(
      String pCollectionId,
      RunnerApi.Components components,
      Pipeline pipeline,
      Map<String, PCollection<?>> visitedPCollections)
      throws IOException {

    PCollection<?> pCollection = visitedPCollections.get(pCollectionId);
    if (pCollection != null) {
      return pCollection;
    }

    RunnerApi.PCollection pCollectionProto = components.getPcollectionsOrThrow(pCollectionId);
    RunnerApi.WindowingStrategy windowingStrategy =
        components.getWindowingStrategiesOrThrow(pCollectionProto.getWindowingStrategyId());

    IsBounded isBounded;
    switch (pCollectionProto.getIsBounded()) {
      case BOUNDED:
        isBounded = IsBounded.BOUNDED;
        break;
      case UNBOUNDED:
        isBounded = IsBounded.UNBOUNDED;
        break;
      case UNRECOGNIZED:
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unknown type of %s: %s",
                RunnerApi.IsBounded.class.getSimpleName(), pCollectionProto.getIsBounded()));
    }

    pCollection =
        PCollection.createPrimitiveOutputInternal(
            pipeline,
            WindowingStrategyTranslation.fromProto(windowingStrategy, components),
            isBounded);

    pCollection.setName(pCollectionId);

    pCollection.setCoder(
        (Coder)
            CoderTranslation.fromProto(
                components.getCodersOrThrow(pCollectionProto.getCoderId()), components));

    visitedPCollections.put(pCollectionId, pCollection);

    return pCollection;
  }

  @AutoValue
  abstract static class RehydratedPTransform
      extends RawPTransform<PInput, POutput> {

    @Nullable
    public abstract String getUrn();

    @Nullable
    public abstract Any getPayload();

    @Override
    public abstract Map<TupleTag<?>, PValue> getAdditionalInputs();

    public static RehydratedPTransform of(
        String urn, Any payload, Map<TupleTag<?>, PValue> additionalInputs) {
      return new AutoValue_PipelineTranslation_RehydratedPTransform(urn, payload, additionalInputs);
    }

    @Override
    public POutput expand(PInput input) {
      throw new IllegalStateException(
          String.format(
              "%s should never be asked to expand;"
                  + " it is the result of deserializing an already-constructed Pipeline",
              getClass().getSimpleName()));
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("urn", getUrn())
          .add("payload", getPayload())
          .toString();
    }
  }
}
