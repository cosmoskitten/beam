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

import java.util.List;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Specialized implementation for
 * {@link org.apache.beam.sdk.transforms.View.AsList View.AsList} for the
 * Flink runner in streaming mode.
 */
public class FlinkStreamingViewAsList<T>
    extends PTransform<PCollection<T>, PCollectionView<List<T>>> {

  @Override
  public PCollectionView<List<T>> expand(PCollection<T> input) {
    PCollectionView<List<T>> view =
        PCollectionViews.listView(
            input.getPipeline(),
            input.getWindowingStrategy(),
            input.getCoder());

    return input.apply(Combine.globally(new Concatenate<T>()).withoutDefaults())
        .apply(FlinkRunner.CreateFlinkPCollectionView.<T, List<T>>of(view));
  }

  @Override
  protected String getKindString() {
    return "StreamingViewAsList";
  }
}
