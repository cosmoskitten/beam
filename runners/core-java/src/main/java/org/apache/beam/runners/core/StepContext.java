package org.apache.beam.runners.core;

import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn.WindowedContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Per-step, per-key context used for retrieving state.
 */
public interface StepContext {

  /**
   * The name of the step.
   */
  String getStepName();

  /**
   * The name of the transform for the step.
   */
  String getTransformName();

  /**
   * Hook for subclasses to implement that will be called whenever
   * {@link WindowedContext#output}
   * is called.
   */
  void noteOutput(WindowedValue<?> output);

  /**
   * Hook for subclasses to implement that will be called whenever
   * {@link WindowedContext#output}
   * is called.
   */
  void noteOutput(TupleTag<?> tag, WindowedValue<?> output);

  /**
   * Writes the given {@code PCollectionView} data to a globally accessible location.
   */
  <T, W extends BoundedWindow> void writePCollectionViewData(
      TupleTag<?> tag,
      Iterable<WindowedValue<T>> data,
      Coder<Iterable<WindowedValue<T>>> dataCoder,
      W window,
      Coder<W> windowCoder)
          throws IOException;

  StateInternals stateInternals();

  TimerInternals timerInternals();
}
