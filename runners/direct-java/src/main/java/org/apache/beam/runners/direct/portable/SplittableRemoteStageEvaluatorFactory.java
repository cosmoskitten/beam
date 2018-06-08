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
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleSplit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleSplit.DelayedApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.ExecutableGraph;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.wire.LengthPrefixUnknownCoders;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * The {@link TransformEvaluatorFactory} for {@link #URN}, which reads from a {@link
 * DirectGroupByKey#DIRECT_GBKO_URN} and feeds the data, using state and timers, to a {@link
 * ExecutableStage} whose first instruction is an SDF.
 */
class SplittableRemoteStageEvaluatorFactory implements TransformEvaluatorFactory {
  public static final String URN = "urn:beam:directrunner:transforms:splittable_remote_stage:v1";

  // A fictional transform that transforms from KWI<unique key, KV<element, restriction>>
  // to simply KV<element, restriction> taken by the SDF inside the ExecutableStage.
  public static final String FEED_SDF_URN = "urn:beam:directrunner:transforms:feed_sdf:v1";

  private final ExecutableGraph<PTransformNode, PCollectionNode> graph;
  private final RehydratedComponents components;
  private final BundleFactory bundleFactory;
  private final JobBundleFactory jobBundleFactory;
  private final StepStateAndTimers.Provider stp;

  SplittableRemoteStageEvaluatorFactory(
      ExecutableGraph<PTransformNode, PCollectionNode> graph,
      Components components,
      BundleFactory bundleFactory,
      JobBundleFactory jobBundleFactory,
      StepStateAndTimers.Provider stepStateAndTimers) {
    this.graph = graph;
    this.components = RehydratedComponents.forComponents(components);
    this.bundleFactory = bundleFactory;
    this.jobBundleFactory = jobBundleFactory;
    this.stp = stepStateAndTimers;
  }

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      PTransformNode application, CommittedBundle<?> inputBundle) throws Exception {
    ExecutableStage stage =
        ExecutableStage.fromPayload(
            ExecutableStagePayload.parseFrom(application.getTransform().getSpec().getPayload()));
    Optional<PTransformNode> speNode =
        stage
            .getTransforms()
            .stream()
            // There can be at most 1.
            .filter(
                t ->
                    t.getTransform()
                        .getSpec()
                        .getUrn()
                        .equals(PTransformTranslation.SPLITTABLE_PROCESS_ELEMENTS_URN))
            .findFirst();
    return new SplittableRemoteStageEvaluator(
        graph,
        speNode.get().getTransform(),
        components,
        bundleFactory,
        jobBundleFactory,
        stp.forStepAndKey(application, inputBundle.getKey()),
        application);
  }

  @Override
  public void cleanup() throws Exception {
    jobBundleFactory.close();
  }

  private static class SplittableRemoteStageEvaluator<InputT, RestrictionT>
      implements TransformEvaluator<KeyedWorkItem<String, KV<InputT, RestrictionT>>> {
    private final ExecutableGraph<PTransformNode, PCollectionNode> graph;
    private final PTransformNode transform;
    private final ExecutableStage stage;
    private final Coder<RestrictionT> restrictionCoder;
    private final Coder<BoundedWindow> windowCoder;
    private final Coder<WindowedValue<KV<InputT, RestrictionT>>> elementRestrictionCoder;

    private static final StateTag<WatermarkHoldState> watermarkHoldTag =
        StateTags.makeSystemTagInternal(
            StateTags.<GlobalWindow>watermarkStateInternal("hold", TimestampCombiner.LATEST));

    private final StateTag<ValueState<WindowedValue<KV<InputT, RestrictionT>>>> seedTag;
    private final StateTag<ValueState<RestrictionT>> restrictionTag;

    private final CopyOnAccessInMemoryStateInternals<String> stateInternals;
    private final DirectTimerInternals timerInternals;
    private final RemoteBundle<KV<InputT, RestrictionT>> bundle;
    private final Collection<UncommittedBundle<?>> outputs;
    private final List<BundleSplit> splits;

    private StateNamespace stateNamespace;
    private ValueState<WindowedValue<KV<InputT, RestrictionT>>> seedState;
    private ValueState<RestrictionT> restrictionState;
    private WatermarkHoldState holdState;
    private Instant inputTimestamp;

    private SplittableRemoteStageEvaluator(
        ExecutableGraph<PTransformNode, PCollectionNode> graph,
        PTransform speTransform,
        RehydratedComponents components,
        BundleFactory bundleFactory,
        JobBundleFactory jobBundleFactory,
        StepStateAndTimers stp,
        PTransformNode transform)
        throws Exception {
      this.graph = graph;
      this.stateInternals = stp.stateInternals();
      this.timerInternals = stp.timerInternals();
      this.transform = transform;
      this.stage =
          ExecutableStage.fromPayload(
              ExecutableStagePayload.parseFrom(transform.getTransform().getSpec().getPayload()));
      this.outputs = new ArrayList<>();
      this.splits = new ArrayList<>();
      StageBundleFactory<KV<InputT, RestrictionT>> stageBundleFactory =
          jobBundleFactory.forStage(stage);
      this.bundle =
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
      {
        String restrictionCoderId =
            ParDoPayload.parseFrom(speTransform.getSpec().getPayload()).getRestrictionCoderId();
        MessageWithComponents message =
            LengthPrefixUnknownCoders.forCoder(
                restrictionCoderId,
                components.getComponents(),
                true /* replaceWithByteArrayCoder */);
        this.restrictionCoder =
            (Coder<RestrictionT>)
                CoderTranslation.fromProto(
                    message.getCoder(),
                    RehydratedComponents.forComponents(message.getComponents()));
      }

      PCollectionNode kwiPcNode =
          Iterables.getOnlyElement(this.graph.getPerElementInputs(transform));
      this.windowCoder =
          (Coder<BoundedWindow>)
              components
                  .getWindowingStrategy(kwiPcNode.getPCollection().getWindowingStrategyId())
                  .getWindowFn()
                  .windowCoder();

      this.elementRestrictionCoder =
          WireCoders.instantiateRunnerWireCoder(stage.getInputPCollection(), stage.getComponents());
      this.seedTag = StateTags.value("seed", elementRestrictionCoder);
      this.restrictionTag = StateTags.value("restriction", this.restrictionCoder);
    }

    @Override
    public void processElement(
        WindowedValue<KeyedWorkItem<String, KV<InputT, RestrictionT>>> windowedWorkItem)
        throws Exception {
      KeyedWorkItem<String, KV<InputT, RestrictionT>> item = windowedWorkItem.getValue();
      TimerInternals.TimerData timer = Iterables.getOnlyElement(item.timersIterable(), null);
      boolean isSeedCall = (timer == null);
      StateNamespace stateNamespace;
      if (isSeedCall) {
        WindowedValue<KV<InputT, RestrictionT>> windowedValue =
            Iterables.getOnlyElement(item.elementsIterable());
        BoundedWindow window = Iterables.getOnlyElement(windowedValue.getWindows());
        stateNamespace = StateNamespaces.window(windowCoder, window);
      } else {
        stateNamespace = timer.getNamespace();
      }
      this.stateNamespace = stateNamespace;

      this.seedState = stateInternals.state(stateNamespace, seedTag);
      this.restrictionState = stateInternals.state(stateNamespace, restrictionTag);
      this.holdState = stateInternals.state(stateNamespace, watermarkHoldTag);

      WindowedValue<KV<InputT, RestrictionT>> elementAndRestriction;
      if (isSeedCall) {
        elementAndRestriction = Iterables.getOnlyElement(item.elementsIterable());
        seedState.write(elementAndRestriction);
      } else {
        // This is not the first ProcessElement call for this element/restriction - rather,
        // this is a timer firing, so we need to fetch the element and restriction from state.
        seedState.readLater();
        restrictionState.readLater();
        WindowedValue<KV<InputT, RestrictionT>> seed = seedState.read();
        elementAndRestriction =
            seed.withValue(KV.of(seed.getValue().getKey(), restrictionState.read()));
      }

      this.inputTimestamp = elementAndRestriction.getTimestamp();
      bundle.getInputReceiver().accept(elementAndRestriction);
    }

    @Override
    public TransformResult<KeyedWorkItem<String, KV<InputT, RestrictionT>>> finishBundle()
        throws Exception {
      bundle.close();

      if (splits.size() > 1) {
        throw new IllegalStateException("More than 1 split per bundle is unsupported for now");
      }
      if (splits.size() == 1) {
        // For now can only happen on the first instruction which is SPLITTABLE_PROCESS_ELEMENTS.
        // Which means we can use .withUnprocessedElements (modulo delay)
        // Alternatively we could have a TransformEvaluator for feeding stuff into an
        // ExecutableStage that starts with an SDF.
        List<DelayedApplication> residuals = splits.get(0).getResidualRootsList();
        checkArgument(residuals.size() == 1, "More than 1 residual is unsupported for now");
        // Residual is a KV<InputT, RestrictionT>.
        DelayedApplication residual = residuals.get(0);
        ByteString encodedResidual = residual.getApplication().getElement();
        WindowedValue<KV<InputT, RestrictionT>> decodedResidual =
            CoderUtils.decodeFromByteArray(elementRestrictionCoder, encodedResidual.toByteArray());
        Duration resumeDelay = new Duration(0L /* residuals.get(0).getResumeDelayMsec() */);

        restrictionState.write(decodedResidual.getValue().getValue());
        Instant watermarkHold =
            residual.getApplication().getOutputWatermarksMap().isEmpty()
                ? inputTimestamp
                : new Instant(
                    Iterables.getOnlyElement(
                        residual.getApplication().getOutputWatermarksMap().values()));
        checkArgument(
            !watermarkHold.isBefore(inputTimestamp),
            "Watermark hold %s can not be before input timestamp %s",
            watermarkHold,
            inputTimestamp);
        Instant wakeupTime = timerInternals.currentProcessingTime().plus(resumeDelay);
        holdState.add(watermarkHold);
        // Set a timer to continue processing this element.
        timerInternals.setTimer(
            stateNamespace, "sdfContinuation", wakeupTime, TimeDomain.PROCESSING_TIME);
      } else {
        seedState.clear();
        restrictionState.clear();
        holdState.clear();
      }
      CopyOnAccessInMemoryStateInternals<String> state = stateInternals.commit();
      StepTransformResult.Builder<KeyedWorkItem<String, KV<InputT, RestrictionT>>> result =
          StepTransformResult.withHold(transform, state.getEarliestWatermarkHold());
      return result
          .addOutput(outputs)
          .withState(state)
          .withTimerUpdate(timerInternals.getTimerUpdate())
          .build();
    }
  }
}
