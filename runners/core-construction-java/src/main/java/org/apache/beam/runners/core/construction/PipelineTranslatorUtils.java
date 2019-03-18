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

import java.io.IOException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableBiMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Sets;

/** Utilities for pipeline translation. */
public final class PipelineTranslatorUtils {
  private PipelineTranslatorUtils() {}

  /** Creates a mapping from PCollection id to output tag integer. */
  public static BiMap<String, Integer> createOutputMap(Iterable<String> localOutputs) {
    ImmutableBiMap.Builder<String, Integer> builder = ImmutableBiMap.builder();
    int outputIndex = 0;
    // sort localOutputs for stable indexing
    for (String tag : Sets.newTreeSet(localOutputs)) {
      builder.put(tag, outputIndex);
      outputIndex++;
    }
    return builder.build();
  }

  public static WindowingStrategy getWindowingStrategy(
      String pCollectionId, RunnerApi.Components components) {
    RunnerApi.WindowingStrategy windowingStrategyProto =
        components.getWindowingStrategiesOrThrow(
            components.getPcollectionsOrThrow(pCollectionId).getWindowingStrategyId());

    try {
      return WindowingStrategyTranslation.fromProto(
          windowingStrategyProto, RehydratedComponents.forComponents(components));
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(
          String.format(
              "Unable to hydrate windowing strategy %s for %s.",
              windowingStrategyProto, pCollectionId),
          e);
    }
  }

  public static ExecutableStagePayload getStagePayload(PTransformNode transformNode) {
    try {
      return RunnerApi.ExecutableStagePayload.parseFrom(
          transformNode.getTransform().getSpec().getPayload());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String getInputId(PTransformNode transformNode) {
    return Iterables.getOnlyElement(transformNode.getTransform().getInputsMap().values());
  }

  public static String getOutputId(PTransformNode transformNode) {
    return Iterables.getOnlyElement(transformNode.getTransform().getOutputsMap().values());
  }
}
