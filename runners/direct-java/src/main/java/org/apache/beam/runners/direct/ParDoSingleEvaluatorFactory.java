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

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/**
 * The {@link DirectRunner} {@link TransformEvaluatorFactory} for the {@link ParDo.Bound} primitive
 * {@link PTransform}.
 */
class ParDoSingleEvaluatorFactory<InputT, OutputT>
    extends ParDoEvaluatorFactoryBase<
        InputT, OutputT, PCollection<OutputT>, ParDo.Bound<InputT, OutputT>> {
  public ParDoSingleEvaluatorFactory(EvaluationContext evaluationContext) {
    super(evaluationContext);
  }

  @Override
  protected DoFn<InputT, OutputT> getDoFn(
      ParDo.Bound<InputT, OutputT> transform) {
    return transform.getNewFn();
  }

  @Override
  protected ParDoEvaluator<InputT, OutputT> createParDoEvaluator(
      AppliedPTransform<PCollection<InputT>, PCollection<OutputT>, ParDo.Bound<InputT, OutputT>>
          application,
      DirectStepContext stepContext,
      DoFn<InputT, OutputT> fnLocal) {
    TupleTag<OutputT> mainOutputTag = new TupleTag<>("out");
    ParDo.Bound<InputT, OutputT> transform = application.getTransform();
    return ParDoEvaluator.create(
        evaluationContext,
        stepContext,
        application,
        fnLocal,
        transform.getSideInputs(),
        mainOutputTag,
        Collections.<TupleTag<?>>emptyList(),
        ImmutableMap.<TupleTag<?>, PCollection<?>>of(mainOutputTag, application.getOutput()));
  }
}
