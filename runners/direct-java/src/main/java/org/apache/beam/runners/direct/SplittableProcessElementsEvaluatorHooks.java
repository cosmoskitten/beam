package org.apache.beam.runners.direct;

import java.util.Collection;
import org.apache.beam.runners.core.ElementAndRestriction;
import org.apache.beam.runners.core.ElementAndRestrictionCoder;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SplittableParDo;
import org.apache.beam.runners.core.SplittableParDo.ProcessElements;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.KeyedWorkItemCoder;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.util.state.StateInternalsFactory;
import org.apache.beam.sdk.util.state.TimerInternalsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/**
 * The {@link TransformEvaluatorFactory} for {@link SplittableParDo.ProcessElements} which is a
 * {@link ParDo}-like transform.
 */
public class SplittableProcessElementsEvaluatorHooks<InputT, OutputT, RestrictionT>
    implements ParDoEvaluatorFactory.TransformHooks<
        KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT,
        PCollectionTuple, ProcessElements<InputT, OutputT, RestrictionT>> {
  @Override
  public DoFn<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT> getDoFn(
      PCollection<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>> input,
      ProcessElements<InputT, OutputT, RestrictionT> transform) {
    ElementAndRestrictionCoder<InputT, RestrictionT> elementAndRestrictionCoder =
        ((ElementAndRestrictionCoder<InputT, RestrictionT>)
            ((KeyedWorkItemCoder<String, ElementAndRestriction<InputT, RestrictionT>>)
                    input.getCoder())
                .getElementCoder());
    return new SplittableParDo.ProcessFn<>(
        transform.getFn(),
        elementAndRestrictionCoder.getElementCoder(),
        elementAndRestrictionCoder.getRestrictionCoder(),
        input.getWindowingStrategy().getWindowFn().windowCoder());
  }

  @Override
  public ParDoEvaluator<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT>
      createParDoEvaluator(
          EvaluationContext evaluationContext,
          AppliedPTransform<
                  PCollection<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>>,
                  PCollectionTuple, ProcessElements<InputT, OutputT, RestrictionT>>
              application,
          final DirectExecutionContext.DirectStepContext stepContext,
          DoFn<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT>
              fnLocal) {
    SplittableParDo.ProcessFn<InputT, OutputT, RestrictionT, ?> processFn =
        (SplittableParDo.ProcessFn<InputT, OutputT, RestrictionT, ?>) fnLocal;
    final ProcessElements<InputT, OutputT, RestrictionT> transform = application.getTransform();
    final ParDoEvaluator<
            KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT>
        res =
            ParDoEvaluator.create(
                evaluationContext,
                stepContext,
                application,
                application.getInput().getWindowingStrategy(),
                processFn,
                transform.getSideInputs(),
                transform.getMainOutputTag(),
                transform.getSideOutputTags().getAll(),
                application.getOutput().getAll());
    processFn.setStateInternalsFactory(
        new StateInternalsFactory<String>() {
          @SuppressWarnings({"unchecked", "rawtypes"})
          @Override
          public StateInternals<String> stateInternalsForKey(String key) {
            return (StateInternals) stepContext.stateInternals();
          }
        });
    processFn.setTimerInternalsFactory(
        new TimerInternalsFactory<String>() {
          @Override
          public TimerInternals timerInternalsForKey(String key) {
            return stepContext.timerInternals();
          }
        });
    processFn.setOutputWindowedValue(
        new OutputWindowedValue<OutputT>() {
          @Override
          public void outputWindowedValue(
              OutputT output,
              Instant timestamp,
              Collection<? extends BoundedWindow> windows,
              PaneInfo pane) {
            res.getOutputManager()
                .output(
                    transform.getMainOutputTag(),
                    WindowedValue.of(output, timestamp, windows, pane));
          }

          @Override
          public <SideOutputT> void sideOutputWindowedValue(
              TupleTag<SideOutputT> tag,
              SideOutputT output,
              Instant timestamp,
              Collection<? extends BoundedWindow> windows,
              PaneInfo pane) {
            res.getOutputManager().output(tag, WindowedValue.of(output, timestamp, windows, pane));
          }
        });
    return res;
  }
}
