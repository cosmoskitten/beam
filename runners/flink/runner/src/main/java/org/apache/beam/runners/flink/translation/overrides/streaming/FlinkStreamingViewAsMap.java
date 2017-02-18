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

import java.util.Map;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Specialized implementation for
 * {@link org.apache.beam.sdk.transforms.View.AsMap View.AsMap}
 * for the Flink runner in streaming mode.
 */
public class FlinkStreamingViewAsMap<K, V>
    extends PTransform<PCollection<KV<K, V>>, PCollectionView<Map<K, V>>> {

  @Override
  public PCollectionView<Map<K, V>> expand(PCollection<KV<K, V>> input) {
    PCollectionView<Map<K, V>> view =
        PCollectionViews.mapView(
            input.getPipeline(),
            input.getWindowingStrategy(),
            input.getCoder());

    @SuppressWarnings({"rawtypes", "unchecked"})
    KvCoder<K, V> inputCoder = (KvCoder) input.getCoder();
    try {
      inputCoder.getKeyCoder().verifyDeterministic();
    } catch (Coder.NonDeterministicException e) {
//      runner.recordViewUsesNonDeterministicKeyCoder(this);
    }

    return input
        .apply(Combine.globally(new Concatenate<KV<K, V>>()).withoutDefaults())
        .apply(FlinkRunner.CreateFlinkPCollectionView.<KV<K, V>, Map<K, V>>of(view));
  }

  @Override
  protected String getKindString() {
    return "StreamingViewAsMap";
  }
}
