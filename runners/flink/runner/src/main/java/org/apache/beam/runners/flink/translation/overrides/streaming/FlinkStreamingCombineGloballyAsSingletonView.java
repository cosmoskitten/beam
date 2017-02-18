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

import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.PCollectionViews;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;


/**
 * Flink Streaming Runner override for {@link Combine.GloballyAsSingletonView}.
 *
 * @param <InputT> the type of the (main) input elements
 * @param <OutputT> the type of the (main) output elements
 */
public class FlinkStreamingCombineGloballyAsSingletonView<InputT, OutputT>
    extends PTransform<PCollection<InputT>, PCollectionView<OutputT>> {

  private static final long serialVersionUID = 1L;

  private Combine.GloballyAsSingletonView<InputT, OutputT> transform;

  /**
   * Builds an instance of this class from the overridden transform.
   */
  @SuppressWarnings("unused") // used via reflection in FlinkRunner#apply()
  FlinkStreamingCombineGloballyAsSingletonView(
      Combine.GloballyAsSingletonView<InputT, OutputT> transform) {
    this.transform = transform;
  }

  @Override
  public PCollectionView<OutputT> expand(PCollection<InputT> input) {
    PCollection<OutputT> combined =
        input.apply(Combine.globally(transform.getCombineFn())
            .withoutDefaults()
            .withFanout(transform.getFanout()));

    PCollectionView<OutputT> view = PCollectionViews.singletonView(
        combined.getPipeline(),
        combined.getWindowingStrategy(),
        transform.getInsertDefault(),
        transform.getInsertDefault()
            ? transform.getCombineFn().defaultValue() : null,
        combined.getCoder());
    return combined
        .apply(ParDo.of(new WrapAsList<OutputT>()))
        .apply(FlinkRunner.CreateFlinkPCollectionView.<OutputT, OutputT>of(view));
  }

  @Override
  protected String getKindString() {
    return "StreamingCombineGloballyAsSingletonView";
  }

  private static class WrapAsList<T> extends DoFn<T, List<T>> {
    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(Collections.singletonList(c.element()));
    }
  }

}
