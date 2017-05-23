package org.apache.beam.runners.core;

/**
 * Per-step, per-key context used for retrieving state.
 */
public interface StepContext {

  StateInternals stateInternals();

  TimerInternals timerInternals();
}
