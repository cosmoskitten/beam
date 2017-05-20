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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.runners.core.construction.PTransforms.FLATTEN_TRANSFORM_URN;
import static org.apache.beam.runners.core.construction.PTransforms.PAR_DO_TRANSFORM_URN;
import static org.apache.beam.runners.core.construction.PTransforms.READ_TRANSFORM_URN;
import static org.apache.beam.runners.core.construction.PTransforms.WINDOW_TRANSFORM_URN;
import static org.apache.beam.runners.direct.DirectGroupByKey.DIRECT_GABW_URN;
import static org.apache.beam.runners.direct.DirectGroupByKey.DIRECT_GBKO_URN;
import static org.apache.beam.runners.direct.ParDoMultiOverrideFactory.DIRECT_STATEFUL_PAR_DO_URN;
import static org.apache.beam.runners.direct.TestStreamEvaluatorFactory.DirectTestStreamFactory.DIRECT_TEST_STREAM_URN;
import static org.apache.beam.runners.direct.ViewOverrideFactory.DIRECT_WRITE_VIEW_URN;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.core.construction.PTransforms;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.runners.direct.TestStreamEvaluatorFactory.DirectTestStreamFactory.DirectTestStream;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TransformEvaluatorFactory} that delegates to primitive {@link TransformEvaluatorFactory}
 * implementations based on the type of {@link PTransform} of the application.
 */
class TransformEvaluatorRegistry implements TransformEvaluatorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TransformEvaluatorRegistry.class);

  private static final String SPLITTABLE_PROCESS_URN =
      "urn:beam:runners_core:transforms:splittable_process:v1";

  public static TransformEvaluatorRegistry defaultRegistry(EvaluationContext ctxt) {
    ImmutableMap<String, TransformEvaluatorFactory> primitives =
        ImmutableMap.<String, TransformEvaluatorFactory>builder()
            // Beam primitives
            .put(READ_TRANSFORM_URN, new BoundedReadEvaluatorFactory(ctxt))
            .put(
                PAR_DO_TRANSFORM_URN,
                new ParDoEvaluatorFactory<>(ctxt, ParDoEvaluator.defaultRunnerFactory()))
            .put(FLATTEN_TRANSFORM_URN, new FlattenEvaluatorFactory(ctxt))
            .put(WINDOW_TRANSFORM_URN, new WindowEvaluatorFactory(ctxt))

            // Runner-specific primitives
            .put(DIRECT_WRITE_VIEW_URN, new ViewEvaluatorFactory(ctxt))
            .put(DIRECT_STATEFUL_PAR_DO_URN, new StatefulParDoEvaluatorFactory<>(ctxt))
            .put(DIRECT_GBKO_URN, new GroupByKeyOnlyEvaluatorFactory(ctxt))
            .put(DIRECT_GABW_URN, new GroupAlsoByWindowEvaluatorFactory(ctxt))
            .put(DIRECT_TEST_STREAM_URN, new TestStreamEvaluatorFactory(ctxt))

            // Runners-core primitives
            .put(SPLITTABLE_PROCESS_URN, new SplittableProcessElementsEvaluatorFactory<>(ctxt))
            .build();
    return new TransformEvaluatorRegistry(primitives);
  }

  /** Registers classes specialized to the direct runner. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class DirectTransformsRegistrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<
        ? extends Class<? extends PTransform>, ? extends PTransforms.TransformPayloadTranslator>
    getTransformPayloadTranslators() {
      return ImmutableMap
          .<Class<? extends PTransform>, PTransforms.TransformPayloadTranslator>builder()
          .put(
              DirectGroupByKey.DirectGroupByKeyOnly.class,
              new PTransforms.RawPTransformTranslator<>())
          .put(
              DirectGroupByKey.DirectGroupAlsoByWindow.class,
              new PTransforms.RawPTransformTranslator())
          .put(
              ParDoMultiOverrideFactory.StatefulParDo.class,
              new PTransforms.RawPTransformTranslator<>())
          .put(
              ViewOverrideFactory.WriteView.class,
              new PTransforms.RawPTransformTranslator<>())
          .put(
              DirectTestStream.class,
              new PTransforms.RawPTransformTranslator<>())
          .build();
    }
  }

  // the TransformEvaluatorFactories can construct instances of all generic types of transform,
  // so all instances of a primitive can be handled with the same evaluator factory.
  private final Map<String, TransformEvaluatorFactory> factories;

  private final AtomicBoolean finished = new AtomicBoolean(false);

  private TransformEvaluatorRegistry(
      @SuppressWarnings("rawtypes")
      Map<String, TransformEvaluatorFactory> factories) {
    this.factories = factories;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle)
      throws Exception {
    checkState(
        !finished.get(), "Tried to get an evaluator for a finished TransformEvaluatorRegistry");

    String urn = PTransforms.urnForTransform(application.getTransform());

    TransformEvaluatorFactory factory =
        checkNotNull(
            factories.get(urn), "No evaluator for PTransform \"%s\"", urn);
    return factory.forApplication(application, inputBundle);
  }

  @Override
  public void cleanup() throws Exception {
    Collection<Exception> thrownInCleanup = new ArrayList<>();
    for (TransformEvaluatorFactory factory : factories.values()) {
      try {
        factory.cleanup();
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        thrownInCleanup.add(e);
      }
    }
    finished.set(true);
    if (!thrownInCleanup.isEmpty()) {
      LOG.error("Exceptions {} thrown while cleaning up evaluators", thrownInCleanup);
      Exception toThrow = null;
      for (Exception e : thrownInCleanup) {
        if (toThrow == null) {
          toThrow = e;
        } else {
          toThrow.addSuppressed(e);
        }
      }
      throw toThrow;
    }
  }
}
