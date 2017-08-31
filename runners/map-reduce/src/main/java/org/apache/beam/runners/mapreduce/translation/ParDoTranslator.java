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
package org.apache.beam.runners.mapreduce.translation;

import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Translates a {@link ParDo} to a {@link Operation}.
 */
class ParDoTranslator<InputT, OutputT>
    extends TransformTranslator.Default<ParDo.MultiOutput<InputT, OutputT>> {

  @Override
  public void translateNode(
      ParDo.MultiOutput<InputT, OutputT> transform, TranslationContext context) {
    TranslationContext.UserGraphContext userGraphContext = context.getUserGraphContext();
    NormalParDoOperation operation = new NormalParDoOperation(
        userGraphContext.getStepName(),
        transform.getFn(),
        userGraphContext.getOptions(),
        transform.getMainOutputTag(),
        transform.getAdditionalOutputTags().getAll(),
        userGraphContext.getSideInputTags(),
        ((PCollection) userGraphContext.getInput()).getWindowingStrategy());
    context.addInitStep(
        Graphs.Step.of(userGraphContext.getStepName(), operation),
        userGraphContext.getInputTags(),
        userGraphContext.getOutputTags());
  }
}
