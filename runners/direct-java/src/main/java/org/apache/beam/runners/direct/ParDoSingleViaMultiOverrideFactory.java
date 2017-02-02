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

import com.google.common.collect.Iterables;
import java.util.List;
import org.apache.beam.runners.core.ParDoSingleViaMulti;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.Bound;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TaggedPValue;

/**
 * A {@link PTransformOverrideFactory} that overrides single-output {@link ParDo} to implement it in
 * terms of multi-output {@link ParDo}.
 */
class ParDoSingleViaMultiOverrideFactory<InputT, OutputT>
    implements PTransformOverrideFactory<
        PCollection<? extends InputT>, PCollection<OutputT>, Bound<InputT, OutputT>> {
  @Override
  public PTransform<PCollection<? extends InputT>, PCollection<OutputT>> getReplacementTransform(
      Bound<InputT, OutputT> transform) {
    return new ParDoSingleViaMulti<>(transform);
  }

  @Override
  public PCollection<? extends InputT> getInput(List<TaggedPValue> inputs, Pipeline p) {
    return (PCollection<? extends InputT>) Iterables.getOnlyElement(inputs).getValue();
  }
}
