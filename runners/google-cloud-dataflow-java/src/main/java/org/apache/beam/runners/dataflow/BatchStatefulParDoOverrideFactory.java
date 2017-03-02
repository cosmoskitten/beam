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
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFns;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;

/**
 * A {@link PTransformOverrideFactory} that provides overrides for applications of a {@link ParDo}
 * in the direct runner. Currently overrides applications of <a
 * href="https://s.apache.org/splittable-do-fn">Splittable DoFn</a>.
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
    if (signature.stateDeclarations().size() > 0 || signature.timerDeclarations().size() > 0) {
      // Based on the fact that the signature is stateful, DoFnSignatures ensures
      // that it is also keyed
      BoundMulti<KV<?, ?>, OutputT> keyedTransform = (BoundMulti<KV<?, ?>, OutputT>) transform;

      return new GbkThenStatefulParDo(keyedTransform);
    } else {
      return transform;
    }
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

          // We are going to GBK to gather keys and windows but otherwise do not want
          // to alter the flow of data. This entails:
          //  - trigger as fast as possible
          //  - maintain the full timestamps of elements
          //  - ensure this GBK holds to the minimum of those timestamps (via OutputTimeFn)
          //  - discard past panes as it is "just a stream" of elements
          .apply(
              Window.<KV<K, WindowedValue<KV<K, InputT>>>>triggering(
                      Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                  .discardingFiredPanes()
                  .withAllowedLateness(inputWindowingStrategy.getAllowedLateness())
                  .withOutputTimeFn(OutputTimeFns.outputAtEarliestInputTimestamp()))

          // A full GBK to group by key _and_ window, not just reshuffle
          .apply("Group by key", GroupByKey.<K, WindowedValue<KV<K, InputT>>>create())

          // Explode the elements
          .apply("Explode groups", ParDo.of(new ExplodeGroupsDoFn<K, InputT>()))

          // Because of the intervening GBK, we may have abused the windowing strategy
          // of the input, which should be transferred to the output in a straightforward manner
          // according to what ParDo already does.
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
