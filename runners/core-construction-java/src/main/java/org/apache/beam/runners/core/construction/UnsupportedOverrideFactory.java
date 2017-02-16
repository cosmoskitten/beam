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

package org.apache.beam.runners.core.construction;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;

/**
 * A {@link PTransformOverrideFactory} that throws an exception when a call to
 * {@link #getReplacementTransform(PTransform)} is made. This is for {@link PTransform PTransforms}
 * which are not supported by a runner.
 */
public final class UnsupportedOverrideFactory<
        InputT extends PInput,
        OutputT extends POutput,
        TransformT extends PTransform<InputT, OutputT>>
    implements PTransformOverrideFactory<InputT, OutputT, TransformT> {

  private final String message;

  @SuppressWarnings("rawtypes")
  public static <
          InputT extends PInput,
          OutputT extends POutput,
          TransformT extends PTransform<InputT, OutputT>>
      UnsupportedOverrideFactory<InputT, OutputT, TransformT> withMessage(String message) {
    return new UnsupportedOverrideFactory<>(message);
  }

  private UnsupportedOverrideFactory(String message) {
    this.message = message;
  }

  @Override
  public PTransform<InputT, OutputT> getReplacementTransform(TransformT transform) {
    throw new IllegalArgumentException(message);
  }

  @Override
  public InputT getInput(
      List<TaggedPValue> inputs, Pipeline p) {
    throw new IllegalArgumentException(message);
  }

  @Override
  public Map<PValue, ReplacementOutput> mapOutputs(
      List<TaggedPValue> outputs, OutputT newOutput) {
    throw new IllegalArgumentException(message);
  }
}
