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

import static com.google.common.collect.Iterables.getOnlyElement;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.ExecutableGraph;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;

class FeedSDFEvaluatorFactory implements TransformEvaluatorFactory {
  private final BundleFactory bundleFactory;
  private final ExecutableGraph<PTransformNode, PCollectionNode> graph;
  private final Components components;
  private final StepStateAndTimers.Provider stp;

  FeedSDFEvaluatorFactory(
      ExecutableGraph<PTransformNode, PCollectionNode> graph,
      Components components,
      BundleFactory bundleFactory,
      StepStateAndTimers.Provider stp) {
    this.bundleFactory = bundleFactory;
    this.graph = graph;
    this.components = components;
    this.stp = stp;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      PTransformNode application, CommittedBundle<?> inputBundle) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    TransformEvaluator<InputT> evaluator =
        createEvaluator(application, (CommittedBundle) inputBundle);
    return evaluator;
  }

  @Override
  public void cleanup() {}

  private <InputT, RestrictionT> FeedSDFEvaluator<InputT, RestrictionT> createEvaluator(
      PTransformNode application, CommittedBundle<KeyedWorkItem<String, KV<InputT, RestrictionT>>> inputBundle) {
    @SuppressWarnings("unchecked")
    StructuralKey<String> key = (StructuralKey<String>) inputBundle.getKey();
    return new FeedSDFEvaluator<>(
        bundleFactory, key, application, graph, components, stp.forStepAndKey(application, key));
  }

  /**
   * A transform evaluator for the pseudo-primitive {@code DirectGroupAlsoByWindow}. The window of
   * the input {@link KeyedWorkItem} is ignored; it should be in the global window, as element
   * windows are reified in the {@link KeyedWorkItem#elementsIterable()}.
   *
   * @see GroupByKeyViaGroupByKeyOnly
   */
  private static class FeedSDFEvaluator<InputT, RestrictionT>
      implements TransformEvaluator<KeyedWorkItem<String, KV<InputT, RestrictionT>>> {
    private final BundleFactory bundleFactory;

    private final PTransformNode application;
    private final PCollectionNode outputCollection;

    private final StructuralKey<String> key;

    private final CopyOnAccessInMemoryStateInternals<String> stateInternals;
    private final DirectTimerInternals timerInternals;
    private final WindowingStrategy<?, BoundedWindow> windowingStrategy;

    private final Collection<UncommittedBundle<?>> outputBundles;

    private FeedSDFEvaluator(
        BundleFactory bundleFactory,
        StructuralKey<String> key,
        PTransformNode application,
        ExecutableGraph<PTransformNode, PCollectionNode> graph,
        Components components,
        StepStateAndTimers<String> stp) {
      this.bundleFactory = bundleFactory;
      this.application = application;
      this.outputCollection = getOnlyElement(graph.getProduced(application));
      this.key = key;

      this.stateInternals = stp.stateInternals();
      this.timerInternals = stp.timerInternals();

      PCollectionNode inputCollection = getOnlyElement(graph.getPerElementInputs(application));
      try {
        windowingStrategy =
            (WindowingStrategy<?, BoundedWindow>)
                RehydratedComponents.forComponents(components)
                    .getWindowingStrategy(
                        inputCollection.getPCollection().getWindowingStrategyId());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      outputBundles = new ArrayList<>();
    }

    @Override
    public void processElement(WindowedValue<KeyedWorkItem<String, KV<InputT, RestrictionT>>> element) throws Exception {
      KeyedWorkItem<String, KV<InputT, RestrictionT>> workItem = element.getValue();

      UncommittedBundle<KV<InputT, RestrictionT>> bundle =
          bundleFactory.createKeyedBundle(this.key, outputCollection);
      if (workItem.elementsIterable().iterator().hasNext()) {
        bundle.add(Iterables.getOnlyElement(workItem.elementsIterable()));
      } else {
        throw new UnsupportedOperationException();
      }
      outputBundles.add(bundle);
    }

    @Override
    public TransformResult<KeyedWorkItem<String, KV<InputT, RestrictionT>>> finishBundle() throws Exception {
      // State is initialized within the constructor. It can never be null.
      CopyOnAccessInMemoryStateInternals<?> state = stateInternals.commit();
      return StepTransformResult.<KeyedWorkItem<String, KV<InputT, RestrictionT>>>withHold(
              application, state.getEarliestWatermarkHold())
          .withState(state)
          .addOutput(outputBundles)
          .withTimerUpdate(timerInternals.getTimerUpdate())
          .build();
    }
  }
}
