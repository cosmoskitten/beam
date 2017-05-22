package org.apache.beam.runners.core;

import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Base class for implementations of {@link StepContext}.
 *
 * <p>To complete a concrete subclass, implement {@link #timerInternals} and
 * {@link #stateInternals}.
 */
public abstract class BaseStepContext implements StepContext {
  private final ExecutionContext executionContext;
  private final String stepName;
  private final String transformName;

  public BaseStepContext(ExecutionContext executionContext, String stepName, String transformName) {
    this.executionContext = executionContext;
    this.stepName = stepName;
    this.transformName = transformName;
  }

  @Override
  public String getStepName() {
    return stepName;
  }

  @Override
  public String getTransformName() {
    return transformName;
  }

  @Override
  public <T, W extends BoundedWindow> void writePCollectionViewData(
      TupleTag<?> tag,
      Iterable<WindowedValue<T>> data, Coder<Iterable<WindowedValue<T>>> dataCoder,
      W window, Coder<W> windowCoder) throws IOException {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public abstract StateInternals stateInternals();

  @Override
  public abstract TimerInternals timerInternals();
}
