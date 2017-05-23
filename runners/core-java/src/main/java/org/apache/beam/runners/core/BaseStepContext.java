package org.apache.beam.runners.core;

/**
 * Base class for implementations of {@link StepContext}.
 *
 * <p>To complete a concrete subclass, implement {@link #timerInternals} and
 * {@link #stateInternals}.
 */
public abstract class BaseStepContext implements StepContext {
  private final String stepName;
  private final String transformName;

  public BaseStepContext(String stepName, String transformName) {
    this.stepName = stepName;
    this.transformName = transformName;
  }

  @Override
  public abstract StateInternals stateInternals();

  @Override
  public abstract TimerInternals timerInternals();
}
