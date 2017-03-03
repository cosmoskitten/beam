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
package org.apache.beam.runners.dataflow;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.BoundMulti;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;

/**
 * A {@link PTransformOverrideFactory} that expands to correctly implement stateful {@link ParDo}
 * using ordinary {@link GroupByKey} to linearize processing per key.
 *
 * <p>This implementation relies on implementation details of the Dataflow runner, specifically
 * standard fusion behavior of {@link ParDo} tranforms following a {@link GroupByKey}.
 */
class BatchStatefulParDoOverrideFactory<InputT, OutputT>
    implements PTransformOverrideFactory<
        PCollection<? extends InputT>, PCollectionTuple, BoundMulti<InputT, OutputT>> {
  @Override
  @SuppressWarnings("unchecked")
  public PTransform<PCollection<? extends InputT>, PCollectionTuple> getReplacementTransform(
      BoundMulti<InputT, OutputT> transform) {

    DoFn<InputT, OutputT> fn = transform.getFn();
    DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());

    // It is still correct to use this without state or timers, but a bad idea.
    // Since it is internal it should never be used wrong, so it is OK to crash.
    checkState(
        signature.usesState() || signature.usesTimers(),
        "%s used for %s that does not use state or timers.",
        getClass().getSimpleName(),
        ParDo.class.getSimpleName());

    BoundMulti<KV<?, ?>, OutputT> keyedTransform = (BoundMulti<KV<?, ?>, OutputT>) transform;
    return new GbkThenStatefulParDo(keyedTransform);
  }

  @Override
  public PCollection<? extends InputT> getInput(List<TaggedPValue> inputs, Pipeline p) {
    return (PCollection<? extends InputT>) Iterables.getOnlyElement(inputs).getValue();
  }

  @Override
  public Map<PValue, ReplacementOutput> mapOutputs(
      List<TaggedPValue> outputs, PCollectionTuple newOutput) {
    return ReplacementOutputs.tagged(outputs, newOutput);
  }

  static class GbkThenStatefulParDo<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, InputT>>, PCollectionTuple> {
    private final BoundMulti<KV<K, InputT>, OutputT> underlyingParDo;

    public GbkThenStatefulParDo(BoundMulti<KV<K, InputT>, OutputT> underlyingParDo) {
      this.underlyingParDo = underlyingParDo;
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<K, InputT>> input) {

      WindowingStrategy<?, ?> inputWindowingStrategy = input.getWindowingStrategy();

      // A KvCoder is required since this goes through GBK. Further, WindowedValueCoder
      // is not registered by default, so we explicitly set the relevant coders.
      checkState(
          input.getCoder() instanceof KvCoder,
          "Input to a %s using state requires a %s, but the coder was %s",
          ParDo.class.getSimpleName(),
          KvCoder.class.getSimpleName(),
          input.getCoder());
      KvCoder<K, InputT> kvCoder = (KvCoder<K, InputT>) input.getCoder();
      Coder<K> keyCoder = kvCoder.getKeyCoder();
      Coder<? extends BoundedWindow> windowCoder =
          inputWindowingStrategy.getWindowFn().windowCoder();

      return input
          // Stash the original timestamps, etc, for when it is fed to the user's DoFn
          .apply("Reify timestamps", ParDo.of(new ReifyWindowedValueFn<K, InputT>()))
          .setCoder(KvCoder.of(keyCoder, WindowedValue.getFullCoder(kvCoder, windowCoder)))

          // A full GBK to group by key _and_ window, not just reshuffle.
          // Since this is batch only, we don't need to alter the triggering.
          .apply("Group by key", GroupByKey.<K, WindowedValue<KV<K, InputT>>>create())

          // Explode the elements
          .apply("Explode groups", ParDo.of(new ExplodeGroupsDoFn<K, InputT>()))
          .setCoder(input.getCoder())

          // Because of the intervening GBK, we need to reset the windowing strategy
          .setWindowingStrategyInternal(inputWindowingStrategy)

          // Do the stateful ParDo as-is now
          .apply("Stateful ParDo", underlyingParDo);
    }
  }

  /** A key-preserving {@link DoFn} that reifies a windowed value. */
  static class ReifyWindowedValueFn<K, V> extends DoFn<KV<K, V>, KV<K, WindowedValue<KV<K, V>>>> {
    @ProcessElement
    public void processElement(final ProcessContext c, final BoundedWindow window) {
      c.output(
          KV.of(
              c.element().getKey(),
              WindowedValue.of(c.element(), c.timestamp(), window, c.pane())));
    }
  }

  /**
   * A key-preserving {@link DoFn} that explodes an iterable that has been grouped by key and
   * window.
   */
  static class ExplodeGroupsDoFn<K, V>
      extends DoFn<KV<K, Iterable<WindowedValue<KV<K, V>>>>, KV<K, V>> {
    @ProcessElement
    public void processElement(final ProcessContext c, final BoundedWindow window) {
      for (WindowedValue<KV<K, V>> windowedValue : c.element().getValue()) {
        c.outputWithTimestamp(windowedValue.getValue(), windowedValue.getTimestamp());
      }
    }
  }
}
