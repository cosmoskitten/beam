package org.apache.beam.runners.core;

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

  StateInternals stateInternals();

  TimerInternals timerInternals();
}
