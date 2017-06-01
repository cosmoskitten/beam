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
package org.apache.beam.runners.flink;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link Pipeline.PipelineVisitor} for overriding user {@link PTransform PTransforms}
 * with {@link FlinkRunner}-specific implementations.
 */
public class PipelineTranslationRewriter extends FlinkPipelineTranslator {

  private final FlinkRunner flinkRunner;
  private final boolean isStreaming;

  public PipelineTranslationRewriter(FlinkRunner flinkRunner) {
    this.flinkRunner = checkNotNull(flinkRunner, "flinkRunner");
    this.isStreaming = flinkRunner.getPipelineOptions().isStreaming();
  }

  @Override
  public void translate(Pipeline pipeline) {
    pipeline.replaceAll(getOverrides());
  }

  private List<PTransformOverride> getOverrides() {
    if (isStreaming) {
      return ImmutableList.<PTransformOverride>builder()
          .add(
              PTransformOverride.of(
                  PTransformMatchers.splittableParDoMulti(),
                  new FlinkStreamingPipelineTranslator.SplittableParDoOverrideFactory()))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(SplittableParDo.ProcessKeyedElements.class),
                  new SplittableParDoViaKeyedWorkItems.OverrideFactory()))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(View.AsIterable.class),
                  new ReflectiveOneToOneOverrideFactory(
                      FlinkStreamingViewOverrides.StreamingViewAsIterable.class, flinkRunner)))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(View.AsList.class),
                  new ReflectiveOneToOneOverrideFactory(
                      FlinkStreamingViewOverrides.StreamingViewAsList.class, flinkRunner)))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(View.AsMap.class),
                  new ReflectiveOneToOneOverrideFactory(
                      FlinkStreamingViewOverrides.StreamingViewAsMap.class, flinkRunner)))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(View.AsMultimap.class),
                  new ReflectiveOneToOneOverrideFactory(
                      FlinkStreamingViewOverrides.StreamingViewAsMultimap.class, flinkRunner)))
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(View.AsSingleton.class),
                  new ReflectiveOneToOneOverrideFactory(
                      FlinkStreamingViewOverrides.StreamingViewAsSingleton.class, flinkRunner)))
          // this has to be last since the ViewAsSingleton override
          // can expand to a Combine.GloballyAsSingletonView
          .add(
              PTransformOverride.of(
                  PTransformMatchers.classEqualTo(Combine.GloballyAsSingletonView.class),
                  new ReflectiveOneToOneOverrideFactory(
                      FlinkStreamingViewOverrides.StreamingCombineGloballyAsSingletonView.class,
                      flinkRunner)))
          .build();
    } else {
      return ImmutableList.of();
    }
  }

  private static class ReflectiveOneToOneOverrideFactory<
      InputT, OutputT, TransformT extends PTransform<PCollection<InputT>, PCollection<OutputT>>>
      extends SingleInputOutputOverrideFactory<
      PCollection<InputT>, PCollection<OutputT>, TransformT> {
    private final Class<PTransform<PCollection<InputT>, PCollection<OutputT>>> replacement;
    private final FlinkRunner runner;

    private ReflectiveOneToOneOverrideFactory(
        Class<PTransform<PCollection<InputT>, PCollection<OutputT>>> replacement,
        FlinkRunner runner) {
      this.replacement = replacement;
      this.runner = runner;
    }

    @Override
    public PTransformReplacement<PCollection<InputT>, PCollection<OutputT>> getReplacementTransform(
        AppliedPTransform<PCollection<InputT>, PCollection<OutputT>, TransformT> transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          InstanceBuilder.ofType(replacement)
              .withArg(FlinkRunner.class, runner)
              .withArg(
                  (Class<PTransform<PCollection<InputT>, PCollection<OutputT>>>)
                      transform.getTransform().getClass(),
                  transform.getTransform())
              .build());
    }
  }
}
