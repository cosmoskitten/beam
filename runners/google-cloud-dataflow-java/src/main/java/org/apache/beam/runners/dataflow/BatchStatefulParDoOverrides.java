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
import org.apache.beam.runners.dataflow.BatchViewOverrides.GroupByKeyAndSortValuesOnly;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
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
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;

/**
 * {@link PTransformOverrideFactory PTransformOverrideFactories} that expands to correctly implement
 * stateful {@link ParDo} using window-unaware {@link GroupByKeyAndSortValuesOnly} to linearize
 * processing per key.
 *
 * <p>This implementation relies on implementation details of the Dataflow runner, specifically
 * standard fusion behavior of {@link ParDo} tranforms following a {@link GroupByKey}.
 */
public class BatchStatefulParDoOverrides {

  /**
   * Returns a {@link PTransformOverrideFactory} that replaces a single-output {@link ParDo} with a
   * composite transform specialized for the {@link DataflowRunner}.
   */
  public static <K, InputT, OutputT>
      PTransformOverrideFactory<
              PCollection<KV<K, InputT>>, PCollection<OutputT>,
              ParDo.SingleOutput<KV<K, InputT>, OutputT>>
          singleOutputOverrideFactory() {
    return new SingleOutputOverrideFactory<>();
  }

  /**
   * Returns a {@link PTransformOverrideFactory} that replaces a multi-output
   * {@link ParDo} with a composite transform specialized for the {@link DataflowRunner}.
   */
  public static <K, InputT, OutputT>
      PTransformOverrideFactory<
              PCollection<KV<K, InputT>>, PCollectionTuple,
              ParDo.BoundMulti<KV<K, InputT>, OutputT>>
          multiOutputOverrideFactory() {
    return new MultiOutputOverrideFactory<>();
  }

  private static class SingleOutputOverrideFactory<K, InputT, OutputT>
      implements PTransformOverrideFactory<
          PCollection<KV<K, InputT>>, PCollection<OutputT>,
          ParDo.SingleOutput<KV<K, InputT>, OutputT>> {

    @Override
    @SuppressWarnings("unchecked")
    public PTransform<PCollection<KV<K, InputT>>, PCollection<OutputT>> getReplacementTransform(
        ParDo.SingleOutput<KV<K, InputT>, OutputT> originalParDo) {
      return new StatefulSingleOutputParDo<>(originalParDo);
    }

    @Override
    public PCollection<KV<K, InputT>> getInput(List<TaggedPValue> inputs, Pipeline p) {
      return (PCollection<KV<K, InputT>>) Iterables.getOnlyElement(inputs).getValue();
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        List<TaggedPValue> outputs, PCollection<OutputT> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  private static class MultiOutputOverrideFactory<K, InputT, OutputT>
      implements PTransformOverrideFactory<
          PCollection<KV<K, InputT>>, PCollectionTuple, ParDo.BoundMulti<KV<K, InputT>, OutputT>> {

    @Override
    @SuppressWarnings("unchecked")
    public PTransform<PCollection<KV<K, InputT>>, PCollectionTuple> getReplacementTransform(
        BoundMulti<KV<K, InputT>, OutputT> originalParDo) {
      return new StatefulMultiOutputParDo<>(originalParDo);
    }

    @Override
    public PCollection<KV<K, InputT>> getInput(List<TaggedPValue> inputs, Pipeline p) {
      return (PCollection<KV<K, InputT>>) Iterables.getOnlyElement(inputs).getValue();
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        List<TaggedPValue> outputs, PCollectionTuple newOutput) {
      return ReplacementOutputs.tagged(outputs, newOutput);
    }
  }

  static class StatefulSingleOutputParDo<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, InputT>>, PCollection<OutputT>> {

    private final ParDo.SingleOutput<KV<K, InputT>, OutputT> originalParDo;

    StatefulSingleOutputParDo(ParDo.SingleOutput<KV<K, InputT>, OutputT> originalParDo) {
      this.originalParDo = originalParDo;
    }

    ParDo.SingleOutput<KV<K, InputT>, OutputT> getOriginalParDo() {
      return originalParDo;
    }

    @Override
    public PCollection<OutputT> expand(PCollection<KV<K, InputT>> input) {
      DoFn<KV<K, InputT>, OutputT> fn = originalParDo.getFn();
      verifyFnIsStateful(fn);

      PTransform<
              PCollection<? extends KV<K, Iterable<KV<Instant, WindowedValue<KV<K, InputT>>>>>>,
              PCollection<OutputT>>
          statefulParDo =
              ParDo.of(new BatchStatefulDoFn<>(fn)).withSideInputs(originalParDo.getSideInputs());

      return input.apply(new GbkBeforeStatefulParDo<K, InputT>()).apply(statefulParDo);
    }
  }

  static class StatefulMultiOutputParDo<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, InputT>>, PCollectionTuple> {

    private final BoundMulti<KV<K, InputT>, OutputT> originalParDo;

    StatefulMultiOutputParDo(ParDo.BoundMulti<KV<K, InputT>, OutputT> originalParDo) {
      this.originalParDo = originalParDo;
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<K, InputT>> input) {
      DoFn<KV<K, InputT>, OutputT> fn = originalParDo.getFn();
      verifyFnIsStateful(fn);

      PTransform<
              PCollection<? extends KV<K, Iterable<KV<Instant, WindowedValue<KV<K, InputT>>>>>>,
              PCollectionTuple>
          statefulParDo =
              ParDo.of(new BatchStatefulDoFn<K, InputT, OutputT>(fn))
                  .withSideInputs(originalParDo.getSideInputs())
                  .withOutputTags(
                      originalParDo.getMainOutputTag(), originalParDo.getSideOutputTags());

      return input.apply(new GbkBeforeStatefulParDo<K, InputT>()).apply(statefulParDo);
    }

    public BoundMulti<KV<K, InputT>, OutputT> getOriginalParDo() {
      return originalParDo;
    }
  }

  static class GbkBeforeStatefulParDo<K, V>
      extends PTransform<
          PCollection<KV<K, V>>,
          PCollection<KV<K, Iterable<KV<Instant, WindowedValue<KV<K, V>>>>>>> {

    @Override
    public PCollection<KV<K, Iterable<KV<Instant, WindowedValue<KV<K, V>>>>>> expand(
        PCollection<KV<K, V>> input) {

      WindowingStrategy<?, ?> inputWindowingStrategy = input.getWindowingStrategy();

      // A KvCoder is required since this goes through GBK. Further, WindowedValueCoder
      // is not registered by default, so we explicitly set the relevant coders.
      checkState(
          input.getCoder() instanceof KvCoder,
          "Input to a %s using state requires a %s, but the coder was %s",
          ParDo.class.getSimpleName(),
          KvCoder.class.getSimpleName(),
          input.getCoder());
      KvCoder<K, V> kvCoder = (KvCoder<K, V>) input.getCoder();
      Coder<K> keyCoder = kvCoder.getKeyCoder();
      Coder<? extends BoundedWindow> windowCoder =
          inputWindowingStrategy.getWindowFn().windowCoder();

      return input
          // Stash the original timestamps, etc, for when it is fed to the user's DoFn
          .apply("ReifyWindows", ParDo.of(new ReifyWindowedValueFn<K, V>()))
          .setCoder(
              KvCoder.of(
                  keyCoder,
                  KvCoder.of(InstantCoder.of(), WindowedValue.getFullCoder(kvCoder, windowCoder))))

          // Group by key and sort by timestamp, dropping windows as they are reified
          .apply(
              "PartitionKeys",
              new GroupByKeyAndSortValuesOnly<K, Instant, WindowedValue<KV<K, V>>>())

          // The GBKO sets the windowing strategy to the global default
          .setWindowingStrategyInternal(inputWindowingStrategy);
    }
  }

  /** A key-preserving {@link DoFn} that reifies a windowed value. */
  static class ReifyWindowedValueFn<K, V>
      extends DoFn<KV<K, V>, KV<K, KV<Instant, WindowedValue<KV<K, V>>>>> {
    @ProcessElement
    public void processElement(final ProcessContext c, final BoundedWindow window) {
      c.output(
          KV.of(
              c.element().getKey(),
              KV.of(
                  c.timestamp(), WindowedValue.of(c.element(), c.timestamp(), window, c.pane()))));
    }
  }

  /**
   * A key-preserving {@link DoFn} that explodes an iterable that has been grouped by key and
   * window.
   */
  public static class BatchStatefulDoFn<K, V, OutputT>
      extends DoFn<KV<K, Iterable<KV<Instant, WindowedValue<KV<K, V>>>>>, OutputT> {

    private final DoFn<KV<K, V>, OutputT> underlyingDoFn;

    BatchStatefulDoFn(DoFn<KV<K, V>, OutputT> underlyingDoFn) {
      this.underlyingDoFn = underlyingDoFn;
    }

    public DoFn<KV<K, V>, OutputT> getUnderlyingDoFn() {
      return underlyingDoFn;
    }

    @ProcessElement
    public void processElement(final ProcessContext c, final BoundedWindow window) {
      throw new UnsupportedOperationException(
          "BatchStatefulDoFn.ProcessElement should never be invoked");
    }

    @Override
    public TypeDescriptor<OutputT> getOutputTypeDescriptor() {
      return underlyingDoFn.getOutputTypeDescriptor();
    }
  }

  private static <InputT, OutputT> void verifyFnIsStateful(DoFn<InputT, OutputT> fn) {
    DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());

    // It is still correct to use this without state or timers, but a bad idea.
    // Since it is internal it should never be used wrong, so it is OK to crash.
    checkState(
        signature.usesState() || signature.usesTimers(),
        "%s used for %s that does not use state or timers.",
        BatchStatefulParDoOverrides.class.getSimpleName(),
        ParDo.class.getSimpleName());
  }
}
