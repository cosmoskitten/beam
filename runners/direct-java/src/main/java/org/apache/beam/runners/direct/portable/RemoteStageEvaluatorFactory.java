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

package org.apache.beam.runners.direct.portable;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleSplit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleSplit.Application;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.ExecutableGraph;
import org.apache.beam.runners.direct.portable.StepTransformResult.Builder;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

/**
 * The {@link TransformEvaluatorFactory} which produces {@link TransformEvaluator evaluators} for
 * stages which execute on an SDK harness via the Fn Execution APIs.
 */
class RemoteStageEvaluatorFactory implements TransformEvaluatorFactory {

  private final ExecutableGraph<PTransformNode, PCollectionNode> graph;
  private final RehydratedComponents components;
  private final BundleFactory bundleFactory;

  private final JobBundleFactory jobBundleFactory;

  RemoteStageEvaluatorFactory(
      ExecutableGraph<PTransformNode, PCollectionNode> graph,
      Components components,
      BundleFactory bundleFactory,
      JobBundleFactory jobBundleFactory) {
    this.graph = graph;
    this.components = RehydratedComponents.forComponents(components);
    this.bundleFactory = bundleFactory;
    this.jobBundleFactory = jobBundleFactory;
  }

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      PTransformNode application, CommittedBundle<?> inputBundle) throws Exception {
    return new RemoteStageEvaluator<>(graph, components, application);
  }

  @Override
  public void cleanup() throws Exception {
    jobBundleFactory.close();
  }

  private class RemoteStageEvaluator<T> implements TransformEvaluator<T> {

    private final ExecutableGraph<PTransformNode, PCollectionNode> graph;
    private final RehydratedComponents components;
    private final PTransformNode transform;
    private final RemoteBundle<T> bundle;
    private final Collection<UncommittedBundle<?>> outputs;
    private final List<BundleSplit> splits;

    private Instant inputTimestamp;

    private RemoteStageEvaluator(
        ExecutableGraph<PTransformNode, PCollectionNode> graph,
        RehydratedComponents components,
        PTransformNode transform)
        throws Exception {
      this.graph = graph;
      this.components = components;
      this.transform = transform;
      ExecutableStage stage =
          ExecutableStage.fromPayload(
              ExecutableStagePayload.parseFrom(transform.getTransform().getSpec().getPayload()));
      outputs = new ArrayList<>();
      splits = new ArrayList<>();
      StageBundleFactory<T> stageBundleFactory = jobBundleFactory.forStage(stage);
      bundle =
          stageBundleFactory.getBundle(
              BundleFactoryOutputReceiverFactory.create(
                  bundleFactory, stage.getComponents(), outputs::add),
              StateRequestHandler.unsupported(),
              new BundleProgressHandler() {
                @Override
                public void onProgress(ProcessBundleProgressResponse progress) {
                  if (progress.hasSplit()) {
                    splits.add(progress.getSplit());
                  }
                }

                @Override
                public void onCompleted(ProcessBundleResponse response) {
                  if (response.hasSplit()) {
                    splits.add(response.getSplit());
                  }
                }
              });
    }

    @Override
    public void processElement(WindowedValue<T> element) throws Exception {
      inputTimestamp = element.getTimestamp();
      bundle.getInputReceiver().accept(element);
    }

    @Override
    public TransformResult<T> finishBundle() throws Exception {
      bundle.close();
      Builder<T> result;

      if (splits.size() > 1) {
        throw new IllegalStateException("More than 1 split per bundle is unsupported for now");
      }
      if (splits.size() == 1) {
        // For now can only happen on the first instruction which is SPLITTABLE_PROCESS_ELEMENTS.
        // Which means we can use .withUnprocessedElements (modulo delay)
        // Alternatively we could have a TransformEvaluator for feeding stuff into an
        // ExecutableStage that starts with an SDF.
        List<Application> residuals = splits.get(0).getResidualRootsList();
        checkArgument(residuals.size() == 1, "More than 1 residual is unsupported for now");
        ByteString residualElement = residuals.get(0).getElement();
        PCollectionNode inputPCNode =
            Iterables.getOnlyElement(this.graph.getPerElementInputs(transform));
        Coder<WindowedValue<T>> coder =
            WireCoders.instantiateRunnerWireCoder(inputPCNode, components.getComponents());

        Instant watermarkHold;
        if (residuals.get(0).getOutputWatermarksMap().isEmpty()) {
          watermarkHold = inputTimestamp;
        } else {
          watermarkHold =
              new Instant(
                  Iterables.getOnlyElement(residuals.get(0).getOutputWatermarksMap().values()));
        }
        result = StepTransformResult.withHold(transform, watermarkHold);
        result.addUnprocessedElements(
            Collections.singletonList(
                CoderUtils.decodeFromByteArray(coder, residualElement.toByteArray())));
      } else {
        result = StepTransformResult.withoutHold(transform);
      }
      result.addOutput(outputs);
      return result.build();
    }
  }
}
