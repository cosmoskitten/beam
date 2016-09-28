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
package org.apache.beam.runners.core;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFns;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateNamespace;
import org.apache.beam.sdk.util.state.StateNamespaces;
import org.apache.beam.sdk.util.state.StateTag;
import org.apache.beam.sdk.util.state.StateTags;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.util.state.WatermarkHoldState;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/**
 * A utility transform that executes a <a
 * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn} by expanding it into a
 * network of simpler transforms:
 *
 * <ol>
 * <li>Pair each element with an initial restriction
 * <li>Split each restriction into sub-restrictions
 * <li>Assign a unique key to each element/restriction pair
 * <li>Group by key (so that work is partitioned by key and we can access state/timers)
 * <li>Process each keyed element/restriction pair with the splittable {@link DoFn}'s {@link
 *     DoFn.ProcessElement} method, using state and timers API.
 * </ol>
 *
 * <p>This transform is intended as a helper for internal use by runners when implementing {@code
 * ParDo.of(splittable DoFn)}, but not for direct use by pipeline writers.
 */
@Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
public class SplittableParDo<
        InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>>
    extends PTransform<PCollection<InputT>, PCollection<OutputT>> {
  private final DoFn<InputT, OutputT> fn;
  private final DoFnSignature signature;

  /**
   * Creates the transform for the given original {@link ParDo} and {@link DoFn}.
   *
   * @param fn The splittable {@link DoFn} inside the original {@link ParDo} transform.
   */
  public SplittableParDo(DoFn<InputT, OutputT> fn) {
    checkNotNull(fn, "fn must not be null");
    this.fn = fn;
    this.signature = DoFnSignatures.INSTANCE.getOrParseSignature(fn.getClass());
    checkArgument(signature.processElement().isSplittable(), "fn must be a splittable DoFn");
  }

  @Override
  public PCollection<OutputT> apply(PCollection<InputT> input) {
    PCollection.IsBounded isFnBounded = signature.isBounded();
    Coder<RestrictionT> restrictionCoder =
        DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn).invokeGetRestrictionCoder();
    Coder<ElementRestriction<InputT, RestrictionT>> splitCoder =
        ElementRestrictionCoder.of(input.getCoder(), restrictionCoder);

    PCollection<KeyedWorkItem<String, ElementRestriction<InputT, RestrictionT>>> keyedWorkItems =
        input
            .apply(
                "Pair with initial restriction",
                ParDo.of(new PairWithRestrictionFn<InputT, OutputT, RestrictionT>(fn)))
            .setCoder(splitCoder)
            .apply("Split restriction", ParDo.of(new SplitRestrictionFn<InputT, RestrictionT>(fn)))
            .setCoder(splitCoder)
            .apply(
                "Assign unique key",
                WithKeys.of(new RandomUniqueKeyFn<ElementRestriction<InputT, RestrictionT>>()))
            .apply(
                "Group by key",
                new GBKIntoKeyedWorkItems<String, ElementRestriction<InputT, RestrictionT>>());
    checkArgument(
        keyedWorkItems.getWindowingStrategy().getWindowFn() instanceof GlobalWindows,
        "GBKIntoKeyedWorkItems must produce a globally windowed collection, "
            + "but windowing strategy was: %s",
        keyedWorkItems.getWindowingStrategy());
    return keyedWorkItems
        .apply(
            "Process",
            ParDo.of(
                new ProcessFn<InputT, OutputT, RestrictionT, TrackerT>(
                    fn,
                    input.getCoder(),
                    input.getWindowingStrategy().getWindowFn().windowCoder())))
        .setIsBoundedInternal(input.isBounded().and(isFnBounded))
        .setWindowingStrategyInternal(input.getWindowingStrategy());
  }

  /**
   * Assigns a random unique key to each element of the input collection, so that the output
   * collection is effectively the same elements as input, but the per-key state and timers are now
   * effectively per-element.
   */
  private static class RandomUniqueKeyFn<T> implements SerializableFunction<T, String> {
    @Override
    public String apply(T input) {
      return UUID.randomUUID().toString();
    }
  }

  /**
   * Pairs each input element with its initial restriction using the given splittable {@link DoFn}.
   */
  private static class PairWithRestrictionFn<InputT, OutputT, RestrictionT>
      extends DoFn<InputT, ElementRestriction<InputT, RestrictionT>> {
    private DoFn<InputT, OutputT> fn;
    private transient DoFnInvoker<InputT, OutputT> invoker;

    PairWithRestrictionFn(DoFn<InputT, OutputT> fn) {
      this.fn = fn;
    }

    @Setup
    public void setup() {
      invoker = DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(
          ElementRestriction.of(
              context.element(),
              invoker.<RestrictionT>invokeGetInitialRestriction(context.element())));
    }
  }

  /**
   * The heart of splittable {@link DoFn} execution: processes a single (element, restriction) pair
   * by creating a tracker for the restriction and checkpointing/resuming processing later if
   * necessary.
   *
   * <p>TODO: This uses deprecated OldDoFn since DoFn does not provide access to state/timer
   * internals. This should be rewritten to use the <a href="https://s.apache.org/beam-state">State
   * and Timers API</a> once it is available.
   */
  @VisibleForTesting
  static class ProcessFn<
          InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>>
      extends OldDoFn<KeyedWorkItem<String, ElementRestriction<InputT, RestrictionT>>, OutputT> {
    /**
     * The state cell containing a watermark hold for the output of this {@link DoFn}. The hold is
     * acquired during the first {@link DoFn.ProcessElement} call for each element and restriction,
     * and is released when the {@link DoFn.ProcessElement} call returns {@link
     * DoFn.ProcessContinuation#stop}.
     *
     * <p>A hold is needed to avoid letting the output watermark immediately progress together with
     * the input watermark when the first {@link DoFn.ProcessElement} call for this element
     * completes.
     *
     * <p>The hold is updated with the future output watermark reported by ProcessContinuation.
     */
    private static final StateTag<Object, WatermarkHoldState<GlobalWindow>> watermarkHoldTag =
        StateTags.makeSystemTagInternal(
            StateTags.<GlobalWindow>watermarkStateInternal(
                "hold", OutputTimeFns.outputAtLatestInputTimestamp()));

    /**
     * The state cell containing a copy of the element. Written during the first {@link
     * DoFn.ProcessElement} call and read during subsequent calls in response to timer firings, when
     * the original element is no longer available.
     */
    private final StateTag<Object, ValueState<WindowedValue<InputT>>> elementTag;

    /**
     * The state cell containing a restriction representing the unprocessed part of work for this
     * element.
     */
    private StateTag<Object, ValueState<RestrictionT>> restrictionTag;

    private final DoFn<InputT, OutputT> fn;
    private final Coder<? extends BoundedWindow> windowCoder;

    private transient DoFnInvoker<InputT, OutputT> invoker;

    ProcessFn(
        DoFn<InputT, OutputT> fn,
        Coder<InputT> elementCoder,
        Coder<? extends BoundedWindow> windowCoder) {
      this.fn = fn;
      this.windowCoder = windowCoder;
      elementTag =
          StateTags.value("element", WindowedValue.getFullCoder(elementCoder, this.windowCoder));
      DoFnInvoker<InputT, OutputT> invoker = DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn);
      restrictionTag =
          StateTags.value("restriction", invoker.<RestrictionT>invokeGetRestrictionCoder());
    }

    @Override
    public void setup() throws Exception {
      invoker = DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn);
    }

    @Override
    public void processElement(final ProcessContext c) {
      // Initialize state (element and restriction) depending on whether this is the seed call.
      // The seed call is the first call for this element, which actually has the element.
      // Subsequent calls are timer firings and the element has to be retrieved from the state.
      TimerInternals.TimerData timer = Iterables.getOnlyElement(c.element().timersIterable(), null);
      boolean isSeedCall = (timer == null);
      StateNamespace stateNamespace = isSeedCall ? StateNamespaces.global() : timer.getNamespace();
      ValueState<WindowedValue<InputT>> elementState =
          c.windowingInternals().stateInternals().state(stateNamespace, elementTag);
      ValueState<RestrictionT> restrictionState =
          c.windowingInternals().stateInternals().state(stateNamespace, restrictionTag);
      WatermarkHoldState<GlobalWindow> holdState =
          c.windowingInternals().stateInternals().state(stateNamespace, watermarkHoldTag);

      ElementRestriction<WindowedValue<InputT>, RestrictionT> elementRestriction;
      if (isSeedCall) {
        // The element and restriction are available in c.element().
        WindowedValue<ElementRestriction<InputT, RestrictionT>> windowedValue =
            Iterables.getOnlyElement(c.element().elementsIterable());
        WindowedValue<InputT> element = windowedValue.withValue(windowedValue.getValue().element());
        elementState.write(element);
        elementRestriction = ElementRestriction.of(element, windowedValue.getValue().restriction());
      } else {
        // This is not the first ProcessElement call for this element/restriction - rather,
        // this is a timer firing, so we need to fetch the element and restriction from state.
        elementState.readLater();
        restrictionState.readLater();
        elementRestriction = ElementRestriction.of(elementState.read(), restrictionState.read());
      }

      final TrackerT tracker = invoker.invokeNewTracker(elementRestriction.restriction());
      @SuppressWarnings("unchecked")
      final RestrictionT[] residual = (RestrictionT[]) new Object[1];
      // TODO: Only let the call run for a limited amount of time, rather than simply
      // producing a limited amount of output.
      DoFn.ProcessContinuation cont =
          invoker.invokeProcessElement(
              makeContext(c, elementRestriction.element(), tracker, residual),
              wrapTracker(tracker));
      if (residual[0] == null) {
        // This means the call completed unsolicited, and the context produced by makeContext()
        // did not take a checkpoint. Take one now.
        residual[0] = checkNotNull(tracker.checkpoint());
      }

      // Save state for resuming.
      if (!cont.shouldResume()) {
        // All work for this element/restriction is completed. Clear state and release hold.
        elementState.clear();
        restrictionState.clear();
        holdState.clear();
        return;
      }
      restrictionState.write(residual[0]);
      Instant futureOutputWatermark = cont.futureOutputWatermark();
      if (futureOutputWatermark != null) {
        holdState.add(futureOutputWatermark);
      }
      // Set a timer to continue processing this element.
      TimerInternals timerInternals = c.windowingInternals().timerInternals();
      timerInternals.setTimer(
          TimerInternals.TimerData.of(
              stateNamespace, Instant.now().plus(cont.resumeDelay()), TimeDomain.PROCESSING_TIME));
    }

    private DoFn<InputT, OutputT>.ProcessContext makeContext(
        final ProcessContext baseContext,
        final WindowedValue<InputT> element,
        final TrackerT tracker,
        final RestrictionT[] residualRestrictionHolder) {
      return fn.new ProcessContext() {
        // Commit at least once every 10k output records.  This keeps the watermark advancing
        // smoothly, and ensures that not too much work will have to be reprocessed in the event of
        // a crash.
        // TODO: Also commit at least once every N seconds (runner-specific parameter).
        private static final int MAX_OUTPUTS_PER_BUNDLE = 10000;

        private int numOutputs = 0;

        public InputT element() {
          return element.getValue();
        }

        public Instant timestamp() {
          return element.getTimestamp();
        }

        public PaneInfo pane() {
          return element.getPane();
        }

        public void output(OutputT output) {
          baseContext
              .windowingInternals()
              .outputWindowedValue(
                  output, element.getTimestamp(), element.getWindows(), element.getPane());
          noteOutput();
        }

        public void outputWithTimestamp(OutputT output, Instant timestamp) {
          baseContext
              .windowingInternals()
              .outputWindowedValue(output, timestamp, element.getWindows(), element.getPane());
          noteOutput();
        }

        private void noteOutput() {
          if (++numOutputs > MAX_OUTPUTS_PER_BUNDLE) {
            // Request a checkpoint. The fn *may* produce more output, but hopefully not too much.
            residualRestrictionHolder[0] = tracker.checkpoint();
          }
        }

        public <T> T sideInput(PCollectionView<T> view) {
          return baseContext.sideInput(view);
        }

        public PipelineOptions getPipelineOptions() {
          return baseContext.getPipelineOptions();
        }

        public <T> void sideOutput(TupleTag<T> tag, T output) {
          // TODO: I'm not sure how to implement this correctly: there's no
          // "internals.sideOutputWindowedValue".
          throw new UnsupportedOperationException(
              "Side outputs not yet supported by splittable DoFn");
        }

        public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
          // TODO: I'm not sure how to implement this correctly: there's no
          // "internals.sideOutputWindowedValue".
          throw new UnsupportedOperationException(
              "Side outputs not yet supported by splittable DoFn");
        }
      };
    }

    /** Creates an {@link DoFn.ExtraContextFactory} that provides just the given tracker. */
    private DoFn.ExtraContextFactory<InputT, OutputT> wrapTracker(final TrackerT tracker) {
      return new DoFn.ExtraContextFactory<InputT, OutputT>() {
        @Override
        public BoundedWindow window() {
          // DoFnSignatures should have verified that this DoFn doesn't access extra context.
          throw new IllegalStateException("Unexpected extra context access on a splittable DoFn");
        }

        @Override
        public DoFn.InputProvider<InputT> inputProvider() {
          // DoFnSignatures should have verified that this DoFn doesn't access extra context.
          throw new IllegalStateException("Unexpected extra context access on a splittable DoFn");
        }

        @Override
        public DoFn.OutputReceiver<OutputT> outputReceiver() {
          // DoFnSignatures should have verified that this DoFn doesn't access extra context.
          throw new IllegalStateException("Unexpected extra context access on a splittable DoFn");
        }

        @Override
        public TrackerT restrictionTracker() {
          return tracker;
        }
      };
    }
  }

  /** Splits the restriction using the given {@link DoFn.SplitRestriction} method. */
  private static class SplitRestrictionFn<InputT, RestrictionT>
      extends DoFn<
          ElementRestriction<InputT, RestrictionT>, ElementRestriction<InputT, RestrictionT>> {
    private final DoFn<InputT, ?> fn;
    private transient DoFnInvoker<InputT, ?> invoker;

    SplitRestrictionFn(DoFn<InputT, ?> fn) {
      this.fn = fn;
    }

    @Setup
    public void setup() {
      invoker = DoFnInvokers.INSTANCE.newByteBuddyInvoker(fn);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      List<RestrictionT> parts =
          invoker.invokeSplitRestriction(c.element().element(), c.element().restriction());
      for (RestrictionT part : parts) {
        c.output(ElementRestriction.of(c.element().element(), part));
      }
    }
  }
}
