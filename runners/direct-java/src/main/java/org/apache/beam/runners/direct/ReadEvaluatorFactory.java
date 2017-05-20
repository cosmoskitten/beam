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

import javax.annotation.Nullable;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link TransformEvaluatorFactory} that produces {@link TransformEvaluator TransformEvaluators}
 * for the {@link Bounded Read.Bounded} primitive {@link PTransform}.
 */
final class ReadEvaluatorFactory implements TransformEvaluatorFactory {

  private final BoundedReadEvaluatorFactory boundedFactory;
  private final UnboundedReadEvaluatorFactory unboundedFactory;

  ReadEvaluatorFactory(EvaluationContext ctx) {
    boundedFactory = new BoundedReadEvaluatorFactory(ctx);
    unboundedFactory = new UnboundedReadEvaluatorFactory(ctx);
  }

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) throws Exception {
    if (inputBundle.getPCollection().isBounded() == PCollection.IsBounded.BOUNDED) {
      return boundedFactory.forApplication(application, inputBundle);
    } else {
      return unboundedFactory.forApplication(application, inputBundle);
    }
  }

  @Override
  public void cleanup() throws Exception {
    boundedFactory.cleanup();
    unboundedFactory.cleanup();
  }
}
