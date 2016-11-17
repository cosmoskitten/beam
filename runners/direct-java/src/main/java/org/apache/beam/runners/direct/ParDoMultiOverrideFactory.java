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
package org.apache.beam.runners.direct;

import org.apache.beam.runners.core.GBKIntoKeyedWorkItems;
import org.apache.beam.runners.core.SplittableParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * A {@link PTransformOverrideFactory} that provides overrides for applications of a {@link ParDo}
 * in the direct runner. Currently overrides applications of <a
 * href="https://s.apache.org/splittable-do-fn">Splittable DoFn</a>.
 */
class ParDoMultiOverrideFactory<InputT, OutputT>
    implements PTransformOverrideFactory<
        PCollection<? extends InputT>, PCollectionTuple, ParDo.BoundMulti<InputT, OutputT>> {

  @Override
  @SuppressWarnings("unchecked")
  public PTransform<PCollection<? extends InputT>, PCollectionTuple> override(
      ParDo.BoundMulti<InputT, OutputT> transform) {

    DoFn<InputT, OutputT> fn = transform.getNewFn();
    DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());
    if (!signature.processElement().isSplittable()) {
      return transform;
    } else {
      return new SplittableParDo(fn);
    }
  }

  static class GbkThenStatefulParDo<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, InputT>>, PCollectionTuple> {
    private final ParDo.BoundMulti<KV<K, InputT>, OutputT> underlyingParDo;

    public GbkThenStatefulParDo(ParDo.BoundMulti<KV<K, InputT>, OutputT> underlyingParDo) {
      this.underlyingParDo = underlyingParDo;
    }

    @Override
    public PCollectionTuple apply(PCollection<KV<K, InputT>> input) {
      return input
          .apply("Group by key", new GBKIntoKeyedWorkItems<K, InputT>())
          .apply("Stateful ParDo", new StatefulParDo<>(underlyingParDo));
    }
  }

  static class StatefulParDo<K, InputT, OutputT>
      extends PTransform<PCollection<? extends KeyedWorkItem<K, InputT>>, PCollectionTuple> {
    private final ParDo.BoundMulti<KV<K, InputT>, OutputT> underlyingParDo;

    public StatefulParDo(ParDo.BoundMulti<KV<K, InputT>, OutputT> underlyingParDo) {
      this.underlyingParDo = underlyingParDo;
    }

    public ParDo.BoundMulti<KV<K, InputT>, OutputT> getUnderlyingParDo() {
      return underlyingParDo;
    }

    public PCollectionTuple apply(PCollection<? extends KeyedWorkItem<K, InputT>> input) {
      return PCollectionTuple.ofPrimitiveOutputsInternal(
          input.getPipeline(),
          TupleTagList.of(underlyingParDo.getMainOutputTag())
              .and(underlyingParDo.getSideOutputTags().getAll()),
          input.getWindowingStrategy(),
          input.isBounded());
    }
  }
}
