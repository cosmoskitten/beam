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

package org.apache.beam.runners.spark.translation;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.runners.spark.metrics.SparkMetricsContainer;
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.runners.spark.util.SparkSideInputReader;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;


/**
 * DoFunctions ignore outputs that are not the main output. MultiDoFunctions deal with additional
 * outputs by enriching the underlying data with multiple TupleTags.
 *
 * @param <InputT> Input type for DoFunction.
 * @param <OutputT> Output type for DoFunction.
 */
public class MultiDoFnFunction<InputT, OutputT>
    implements PairFlatMapFunction<Iterator<WindowedValue<InputT>>, TupleTag<?>, WindowedValue<?>> {

  private final Accumulator<NamedAggregators> aggAccum;
  private final Accumulator<SparkMetricsContainer> metricsAccum;
  private final String stepName;
  private final DoFn<InputT, OutputT> doFn;
  private final SparkRuntimeContext runtimeContext;
  private final TupleTag<OutputT> mainOutputTag;
  private final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final boolean stateful;

  /**
   * @param aggAccum       The Spark {@link Accumulator} that backs the Beam Aggregators.
   * @param metricsAccum       The Spark {@link Accumulator} that backs the Beam metrics.
   * @param doFn              The {@link DoFn} to be wrapped.
   * @param runtimeContext    The {@link SparkRuntimeContext}.
   * @param mainOutputTag     The main output {@link TupleTag}.
   * @param sideInputs        Side inputs used in this {@link DoFn}.
   * @param windowingStrategy Input {@link WindowingStrategy}.
   */
  public MultiDoFnFunction(
      Accumulator<NamedAggregators> aggAccum,
      Accumulator<SparkMetricsContainer> metricsAccum,
      String stepName,
      DoFn<InputT, OutputT> doFn,
      SparkRuntimeContext runtimeContext,
      TupleTag<OutputT> mainOutputTag,
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs,
      WindowingStrategy<?, ?> windowingStrategy) {
    this.aggAccum = aggAccum;
    this.metricsAccum = metricsAccum;
    this.stepName = stepName;
    this.doFn = doFn;
    this.runtimeContext = runtimeContext;
    this.mainOutputTag = mainOutputTag;
    this.sideInputs = sideInputs;
    this.windowingStrategy = windowingStrategy;
    DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
    stateful = signature.stateDeclarations().size() > 0 || signature.timerDeclarations().size() > 0;
  }

  @Override
  public Iterable<Tuple2<TupleTag<?>, WindowedValue<?>>> call(
      Iterator<WindowedValue<InputT>> iter) throws Exception {

    DoFnOutputManager outputManager = new DoFnOutputManager();

    SparkProcessContext.NoOpStepContext context;
    InMemoryTimerInternals timerInternals = null;
    // Now only implements the StatefulParDo in Batch mode.
    if (stateful) {
      Object key = null;
      if (iter.hasNext()) {
        WindowedValue<InputT> currentValue = iter.next();
        key = ((KV) currentValue.getValue()).getKey();
        iter = Iterators.concat(Iterators.singletonIterator(currentValue), iter);
      }
      final InMemoryStateInternals<?> stateInternals = InMemoryStateInternals.forKey(key);
      timerInternals = new InMemoryTimerInternals();
      final InMemoryTimerInternals finalTimerInternals = timerInternals;
      context = new SparkProcessContext.NoOpStepContext(){
        @Override
        public StateInternals stateInternals() {
          return stateInternals;
        }

        @Override
        public TimerInternals timerInternals() {
          return finalTimerInternals;
        }
      };
    } else {
      context = new SparkProcessContext.NoOpStepContext();
    }

    final DoFnRunner<InputT, OutputT> doFnRunner =
        DoFnRunners.simpleRunner(
            runtimeContext.getPipelineOptions(),
            doFn,
            new SparkSideInputReader(sideInputs),
            outputManager,
            mainOutputTag,
            Collections.<TupleTag<?>>emptyList(),
            context,
            windowingStrategy);

    DoFnRunnerWithMetrics<InputT, OutputT> doFnRunnerWithMetrics =
        new DoFnRunnerWithMetrics<>(stepName, doFnRunner, metricsAccum);

    return new SparkProcessContext<>(
        doFn, doFnRunnerWithMetrics, outputManager,
        stateful ? new TimerDataIterator(timerInternals) :
            Collections.<TimerInternals.TimerData>emptyIterator()).processPartition(iter);
  }

  private static class TimerDataIterator implements Iterator<TimerInternals.TimerData> {

    private InMemoryTimerInternals timerInternals;
    private boolean hasAdvance;
    private TimerInternals.TimerData timerData;

    TimerDataIterator(InMemoryTimerInternals timerInternals) {
      this.timerInternals = timerInternals;
    }

    @Override
    public boolean hasNext() {

      // Advance
      if (!hasAdvance) {
        try {
          // Finish any pending windows by advancing the input watermark to infinity.
          timerInternals.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);
          // Finally, advance the processing time to infinity to fire any timers.
          timerInternals.advanceProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
          timerInternals.advanceSynchronizedProcessingTime(
              BoundedWindow.TIMESTAMP_MAX_VALUE);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        hasAdvance = true;
      }

      // Get timer data
      return (timerData = timerInternals.removeNextEventTimer()) != null
          || (timerData = timerInternals.removeNextProcessingTimer()) != null
          || (timerData = timerInternals.removeNextSynchronizedProcessingTimer()) != null;
    }

    @Override
    public TimerInternals.TimerData next() {
      if (timerData == null) {
        throw new NoSuchElementException();
      } else {
        return timerData;
      }
    }

    @Override
    public void remove() {
      throw new RuntimeException("TimerDataIterator not support remove!");
    }

  }

  private class DoFnOutputManager
      implements SparkProcessContext.SparkOutputManager<Tuple2<TupleTag<?>, WindowedValue<?>>> {

    private final Multimap<TupleTag<?>, WindowedValue<?>> outputs = LinkedListMultimap.create();

    @Override
    public void clear() {
      outputs.clear();
    }

    @Override
    public Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> iterator() {
      Iterator<Map.Entry<TupleTag<?>, WindowedValue<?>>> entryIter = outputs.entries().iterator();
      return Iterators.transform(entryIter, this.<TupleTag<?>, WindowedValue<?>>entryToTupleFn());
    }

    private <K, V> Function<Map.Entry<K, V>, Tuple2<K, V>> entryToTupleFn() {
      return new Function<Map.Entry<K, V>, Tuple2<K, V>>() {
        @Override
        public Tuple2<K, V> apply(Map.Entry<K, V> en) {
          return new Tuple2<>(en.getKey(), en.getValue());
        }
      };
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      outputs.put(tag, output);
    }
  }
}
