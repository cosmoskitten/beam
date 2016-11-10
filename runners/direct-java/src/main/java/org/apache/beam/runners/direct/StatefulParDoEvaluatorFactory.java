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

import com.google.auto.value.AutoValue;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.ParDoMultiOverrideFactory.StatefulParDo;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.StateDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateNamespace;
import org.apache.beam.sdk.util.state.StateNamespaces;
import org.apache.beam.sdk.util.state.StateSpec;
import org.apache.beam.sdk.util.state.StateTag;
import org.apache.beam.sdk.util.state.StateTags;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link TransformEvaluatorFactory} for {@link ParDo.BoundMulti}. */
final class StatefulParDoEvaluatorFactory<K, InputT, OutputT> implements TransformEvaluatorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(StatefulParDoEvaluatorFactory.class);
  private final EvaluationContext evaluationContext;
  private final LoadingCache<AppliedPTransformOutputKeyAndWindow<K, InputT, OutputT>, Runnable>
      cleanupRegistry;

  private final ParDoEvaluatorFactory delegate;

  StatefulParDoEvaluatorFactory(EvaluationContext evaluationContext) {
    this.delegate = new ParDoEvaluatorFactory(evaluationContext);
    this.evaluationContext = evaluationContext;
    this.cleanupRegistry =
        CacheBuilder.newBuilder()
            .weakValues()
            .build(new CleanupSchedulingLoader(evaluationContext));
  }

  @Override
  public <T> TransformEvaluator<T> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) throws Exception {
    @SuppressWarnings({"unchecked", "rawtypes"})
    TransformEvaluator<T> evaluator =
        (TransformEvaluator<T>)
            createEvaluator((AppliedPTransform) application, (CommittedBundle) inputBundle);
    return evaluator;
  }

  @Override
  public void cleanup() throws Exception {
    delegate.cleanup();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private TransformEvaluator<InputT> createEvaluator(
      AppliedPTransform<
              PCollection<? extends KV<K, Iterable<InputT>>>, PCollectionTuple,
              StatefulParDo<K, InputT, OutputT>>
          application,
      CommittedBundle<KV<K, Iterable<InputT>>> inputBundle)
      throws Exception {
    String stepName = evaluationContext.getStepName(application);
    final DirectStepContext stepContext =
        evaluationContext
            .getExecutionContext(application, inputBundle.getKey())
            .getOrCreateStepContext(stepName, stepName);

    final DoFn<?, ?> doFn = application.getTransform().getUnderlyingParDo().getNewFn();
    final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());

    // If the DoFn is stateful, schedule state clearing.
    // It is semantically correct to schedule any number of redundant clear tasks; the
    // cache is used to limit the number of tasks to avoid performance degradation.
    if (signature.stateDeclarations().size() > 0) {
      for (final WindowedValue<?> element : inputBundle.getElements()) {
        for (final BoundedWindow window : element.getWindows()) {
          cleanupRegistry.get(
              AppliedPTransformOutputKeyAndWindow.create(
                  application, (StructuralKey<K>) inputBundle.getKey(), window));
        }
      }
    }

    TransformEvaluator<KV<K, InputT>> delegateEvaluator =
        delegate.createEvaluator(
            application,
            inputBundle,
            doFn,
            application.getTransform().getUnderlyingParDo().getSideInputs(),
            application.getTransform().getUnderlyingParDo().getMainOutputTag(),
            application.getTransform().getUnderlyingParDo().getSideOutputTags().getAll());

    return new StatefulParDoEvaluator(delegateEvaluator);
  }

  private class CleanupSchedulingLoader
      extends CacheLoader<AppliedPTransformOutputKeyAndWindow<K, InputT, OutputT>, Runnable> {

    private final EvaluationContext evaluationContext;

    public CleanupSchedulingLoader(EvaluationContext evaluationContext) {
      this.evaluationContext = evaluationContext;
    }

    @Override
    public Runnable load(
        final AppliedPTransformOutputKeyAndWindow<K, InputT, OutputT> transformOutputWindow) {
      String stepName = evaluationContext.getStepName(transformOutputWindow.getTransform());

      PCollection<?> pc =
          transformOutputWindow
              .getTransform()
              .getOutput()
              .get(
                  transformOutputWindow
                      .getTransform()
                      .getTransform()
                      .getUnderlyingParDo()
                      .getMainOutputTag());
      WindowingStrategy<?, ?> windowingStrategy = pc.getWindowingStrategy();
      BoundedWindow window = transformOutputWindow.getWindow();
      final DoFn<?, ?> doFn =
          transformOutputWindow.getTransform().getTransform().getUnderlyingParDo().getNewFn();
      final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());

      final DirectStepContext stepContext =
          evaluationContext
              .getExecutionContext(
                  transformOutputWindow.getTransform(), transformOutputWindow.getKey())
              .getOrCreateStepContext(stepName, stepName);

      final StateNamespace namespace =
          StateNamespaces.window(
              (Coder<BoundedWindow>) windowingStrategy.getWindowFn().windowCoder(), window);

      Runnable cleanup =
          new Runnable() {
            @Override
            public void run() {
              for (StateDeclaration stateDecl : signature.stateDeclarations().values()) {
                StateTag<Object, ?> tag;
                try {
                  tag =
                      StateTags.tagForSpec(stateDecl.id(), (StateSpec) stateDecl.field().get(doFn));
                } catch (IllegalAccessException e) {
                  throw new RuntimeException(
                      String.format(
                          "Error accessing %s for %s",
                          StateSpec.class.getName(), doFn.getClass().getName()),
                      e);
                }
                stepContext.stateInternals().state(namespace, tag).clear();
              }
              cleanupRegistry.invalidate(transformOutputWindow);
            }
          };

      evaluationContext.scheduleAfterWindowExpiration(
          transformOutputWindow.getTransform(), window, windowingStrategy, cleanup);
      return cleanup;
    }
  }

  @AutoValue
  abstract static class AppliedPTransformOutputKeyAndWindow<K, InputT, OutputT> {
    abstract AppliedPTransform<
            PCollection<? extends KV<K, Iterable<InputT>>>, PCollectionTuple,
            StatefulParDo<K, InputT, OutputT>>
        getTransform();

    abstract StructuralKey<K> getKey();

    abstract BoundedWindow getWindow();

    static <K, InputT, OutputT> AppliedPTransformOutputKeyAndWindow create(
        AppliedPTransform<
                PCollection<? extends KV<K, Iterable<InputT>>>, PCollectionTuple,
                StatefulParDo<K, InputT, OutputT>>
            transform,
        StructuralKey<K> key,
        BoundedWindow w) {
      return new AutoValue_StatefulParDoEvaluatorFactory_AppliedPTransformOutputKeyAndWindow<>(
          transform, key, w);
    }
  }

  private static class StatefulParDoEvaluator<K, InputT>
      implements TransformEvaluator<KV<K, Iterable<InputT>>> {

    private final TransformEvaluator<KV<K, InputT>> delegateEvaluator;

    public StatefulParDoEvaluator(TransformEvaluator<KV<K, InputT>> delegateEvaluator) {
      this.delegateEvaluator = delegateEvaluator;
    }

    @Override
    public void processElement(WindowedValue<KV<K, Iterable<InputT>>> gbkResult)
        throws Exception {

      for (InputT value : gbkResult.getValue().getValue()) {
        delegateEvaluator.processElement(
            gbkResult.withValue(KV.of(gbkResult.getValue().getKey(), value)));
      }
    }

    @Override
    public TransformResult finishBundle() throws Exception {
      return delegateEvaluator.finishBundle();
    }
  }
}
