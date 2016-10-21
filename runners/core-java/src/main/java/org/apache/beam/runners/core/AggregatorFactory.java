package org.apache.beam.runners.core;

import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.ExecutionContext;

/**
 * A factory for creating aggregators.
 */
public interface AggregatorFactory {
  /**
   * Create an aggregator with the given {@code name} and {@link CombineFn}.
   *
   *  <p>This method is called to create an aggregator for a {@link DoFn}. It receives the
   *  class of the {@link DoFn} being executed and the context of the step it is being
   *  executed in.
   */
  <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
      Class<?> fnClass, ExecutionContext.StepContext stepContext,
      String aggregatorName, CombineFn<InputT, AccumT, OutputT> combine);
}
