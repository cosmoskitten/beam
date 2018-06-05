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
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.SplittableProcessElementInvoker;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.ExecutableGraph;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

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
      PTransformNode application,
      CommittedBundle<KeyedWorkItem<String, KV<InputT, RestrictionT>>> inputBundle) {
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

    /**
     * The state cell containing a watermark hold for the output of this {@link DoFn}. The hold is
     * acquired during the first {@link DoFn.ProcessElement} call for each element and restriction,
     * and is released when the {@link DoFn.ProcessElement} call returns {@link
     * ProcessContinuation#stop()}.
     *
     * <p>A hold is needed to avoid letting the output watermark immediately progress together with
     * the input watermark when the first {@link DoFn.ProcessElement} call for this element
     * completes.
     */
    private static final StateTag<WatermarkHoldState> watermarkHoldTag =
        StateTags.makeSystemTagInternal(
            StateTags.<GlobalWindow>watermarkStateInternal("hold", TimestampCombiner.LATEST));

    /**
     * The state cell containing a copy of the element. Written during the first {@link
     * DoFn.ProcessElement} call and read during subsequent calls in response to timer firings, when
     * the original element is no longer available.
     */
    private final StateTag<ValueState<WindowedValue<InputT>>> elementTag;

    /**
     * The state cell containing a restriction representing the unprocessed part of work for this
     * element.
     */
    private StateTag<ValueState<RestrictionT>> restrictionTag;

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

      Coder<InputT> elementCoder;
      Coder<RestrictionT> restrictionCoder;

      try {
        ParDoPayload payload =
            ParDoPayload.parseFrom(application.getTransform().getSpec().getPayload());
        restrictionCoder =
            (Coder<RestrictionT>)
                CoderTranslation.fromProto(
                    components.getCodersOrThrow(payload.getRestrictionCoderId()),
                    RehydratedComponents.forComponents(components));
        elementCoder =
            (Coder<InputT>)
                CoderTranslation.fromProto(
                    components.getCodersOrThrow(inputCollection.getPCollection().getCoderId()),
                    RehydratedComponents.forComponents(components));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      this.elementTag =
          StateTags.value(
              "element",
              WindowedValue.getFullCoder(
                  elementCoder, windowingStrategy.getWindowFn().windowCoder()));
      this.restrictionTag = StateTags.value("restriction", restrictionCoder);

      outputBundles = new ArrayList<>();
    }

    @Override
    public void processElement(
        WindowedValue<KeyedWorkItem<String, KV<InputT, RestrictionT>>> windowedWorkItem)
        throws Exception {
      KeyedWorkItem<String, KV<InputT, RestrictionT>> workItem = windowedWorkItem.getValue();
      WindowedValue<KV<InputT, RestrictionT>> windowedValue =
          Iterables.getOnlyElement(workItem.elementsIterable());
      WindowedValue<InputT> element = windowedValue.withValue(windowedValue.getValue().getKey());
      KV<WindowedValue<InputT>, RestrictionT> elementAndRestriction =
          KV.of(element, windowedValue.getValue().getValue());

      UncommittedBundle<KV<InputT, RestrictionT>> bundle =
          bundleFactory.createKeyedBundle(this.key, outputCollection);
      bundle.add(
          elementAndRestriction
              .getKey()
              .withValue(
                  KV.of(
                      elementAndRestriction.getKey().getValue(),
                      elementAndRestriction.getValue())));
      outputBundles.add(bundle);
    }

    @Override
    public TransformResult<KeyedWorkItem<String, KV<InputT, RestrictionT>>> finishBundle()
        throws Exception {
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
