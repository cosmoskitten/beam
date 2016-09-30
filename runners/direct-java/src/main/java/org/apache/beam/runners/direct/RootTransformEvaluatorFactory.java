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

import java.util.Collection;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * A {@link TransformEvaluatorFactory} for {@link PTransform PTransforms} that are at the root of a
 * {@link Pipeline}. Provides a way to get initial inputs, which will cause the {@link PTransform}
 * to produce all of the appropriate output.
 */
interface RootTransformEvaluatorFactory extends TransformEvaluatorFactory {
  /**
   * Get the initial inputs for the {@link AppliedPTransform}. The {@link AppliedPTransform} will be
   * provided with these {@link CommittedBundle bundles} as input when the {@link Pipeline} runs.
   *
   * <p>For source transforms, these should be sufficient that, when provided to the evaluators
   * produced by {@link #forApplication(AppliedPTransform, CommittedBundle)}, all of the elements
   * contained in the source are eventually produced.
   */
  Collection<CommittedBundle<?>> getInitialInputs(AppliedPTransform<?, ?, ?> transform);
}
