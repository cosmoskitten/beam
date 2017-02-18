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
package org.apache.beam.runners.flink.translation.overrides.streaming;

import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;

/**
 * A Flink Runner-specific {@link PTransformOverrideFactory} for
 * {@link View.AsMultimap} PTransforms.
 */
public final class FlinkStreamingViewAsMultimapOverrideFactory<K, V>
    implements PTransformOverrideFactory<
      PCollection<KV<K, V>>,
      PCollectionView<Map<K, Iterable<V>>>,
    View.AsMultimap<K, V>> {

  @Override
  public PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, Iterable<V>>>>
      getReplacementTransform(View.AsMultimap<K, V> transform) {
    return new FlinkStreamingViewAsMultimap<>();
  }

  @Override
  public PCollection<KV<K, V>> getInput(
      List<TaggedPValue> inputs, Pipeline p) {
    return (PCollection<KV<K, V>>) Iterables.getOnlyElement(inputs).getValue();
  }

  @Override
  public Map<PValue, ReplacementOutput> mapOutputs(
      List<TaggedPValue> outputs, PCollectionView<Map<K, Iterable<V>>> newOutput) {
    return ReplacementOutputs.singleton(outputs, newOutput);
  }
}
