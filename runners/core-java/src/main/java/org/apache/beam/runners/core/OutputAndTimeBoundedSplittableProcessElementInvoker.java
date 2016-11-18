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

import com.google.common.collect.Iterables;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link SplittableProcessElementInvoker} that requests a checkpoint after the {@link
 * DoFn.ProcessElement} call either outputs at least a given number of elements (in total over all
 * outputs), or runs for the given duration.
 */
public class OutputAndTimeBoundedSplittableProcessElementInvoker<
        InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>>
    extends SplittableProcessElementInvoker<InputT, OutputT, RestrictionT, TrackerT> {
  private final DoFn<InputT, OutputT> fn;
  private final PipelineOptions pipelineOptions;
  private final OutputWindowedValue<OutputT> output;
  private final SideInputReader sideInputReader;
  private final ScheduledExecutorService executor;
  private final int maxNumOutputs;
  private final Duration maxDuration;

  /**
   * Creates a new invoker from components.
   *
   * @param fn The original {@link DoFn}.
   * @param pipelineOptions {@link PipelineOptions} to include in the {@link DoFn.ProcessContext}.
   * @param output Hook for outputting from the {@link DoFn.ProcessElement} method.
   * @param sideInputReader Hook for accessing side inputs.
   * @param executor Executor on which a checkpoint will be scheduled after the given duration.
   * @param maxNumOutputs Maximum number of outputs, in total over all output tags, after which a
   *     checkpoint will be requested. This is a best-effort request - the {@link DoFn} may output
   *     more after receiving the request.
   * @param maxDuration Maximum duration of the {@link DoFn.ProcessElement} call after which a
   *     checkpoint will be requested. This is a best-effort request - the {@link DoFn} may run for
   *     longer after receiving the request.
   */
  public OutputAndTimeBoundedSplittableProcessElementInvoker(
      DoFn<InputT, OutputT> fn,
      PipelineOptions pipelineOptions,
      OutputWindowedValue<OutputT> output,
      SideInputReader sideInputReader,
      ScheduledExecutorService executor,
      int maxNumOutputs,
      Duration maxDuration) {
    this.fn = fn;
    this.pipelineOptions = pipelineOptions;
    this.output = output;
    this.sideInputReader = sideInputReader;
    this.executor = executor;
    this.maxNumOutputs = maxNumOutputs;
    this.maxDuration = maxDuration;
  }

  @Override
  public Result invokeProcessElement(
      DoFnInvoker<InputT, OutputT> invoker,
      final WindowedValue<InputT> element,
      final TrackerT tracker) {
    final ProcessContext processContext = new ProcessContext(element, tracker);
    DoFn.ProcessContinuation cont =
        invoker.invokeProcessElement(
            new DoFnInvoker.FakeArgumentProvider<InputT, OutputT>() {
              @Override
              public DoFn<InputT, OutputT>.ProcessContext processContext(
                  DoFn<InputT, OutputT> doFn) {
                return processContext;
              }

              @Override
              public RestrictionTracker<?> restrictionTracker() {
                return tracker;
              }
            });
    RestrictionT residual;
    RestrictionT forcedCheckpoint = processContext.extractCheckpoint();
    if (forcedCheckpoint == null) {
      // If no checkpoint was forced, the call returned voluntarily - but we still need
      // to have a checkpoint to potentially resume from.
      residual = tracker.checkpoint();
    } else {
      residual = forcedCheckpoint;
    }
    return new Result(residual, cont);
  }

  private class ProcessContext extends DoFn<InputT, OutputT>.ProcessContext {
    private final WindowedValue<InputT> element;
    private final TrackerT tracker;

    private int numOutputs;
    private RestrictionT checkpoint;

    private Future<?> scheduledCheckpoint;

    public ProcessContext(WindowedValue<InputT> element, TrackerT tracker) {
      fn.super();
      this.element = element;
      this.tracker = tracker;

      this.scheduledCheckpoint =
          executor.schedule(
              new Runnable() {
                @Override
                public void run() {
                  initiateCheckpoint();
                }
              },
              maxDuration.getMillis(),
              TimeUnit.MILLISECONDS);
    }

    @Nullable
    RestrictionT extractCheckpoint() {
      scheduledCheckpoint.cancel(true);
      try {
        scheduledCheckpoint.get();
      } catch (InterruptedException | ExecutionException | CancellationException e) {
        // Ignore
      }
      synchronized (this) {
        return checkpoint;
      }
    }

    @Override
    public InputT element() {
      return element.getValue();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      return sideInputReader.get(
          view,
          view.getWindowingStrategyInternal()
              .getWindowFn()
              .getSideInputWindow(Iterables.getOnlyElement(element.getWindows())));
    }

    @Override
    public Instant timestamp() {
      return element.getTimestamp();
    }

    @Override
    public PaneInfo pane() {
      return element.getPane();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public void output(OutputT output) {
      outputWithTimestamp(output, element.getTimestamp());
    }

    @Override
    public void outputWithTimestamp(OutputT value, Instant timestamp) {
      output.outputWindowedValue(value, timestamp, element.getWindows(), element.getPane());
      noteOutput();
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T value) {
      sideOutputWithTimestamp(tag, value, element.getTimestamp());
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T value, Instant timestamp) {
      output.sideOutputWindowedValue(
          tag, value, timestamp, element.getWindows(), element.getPane());
      noteOutput();
    }

    private void noteOutput() {
      ++numOutputs;
      if (numOutputs >= maxNumOutputs) {
        initiateCheckpoint();
      }
    }

    private synchronized void initiateCheckpoint() {
      if (checkpoint == null) {
        checkpoint = tracker.checkpoint();
      }
    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregator(
        String name, Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
      throw new UnsupportedOperationException();
    }
  }
}
