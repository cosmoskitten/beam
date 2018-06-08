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
package org.apache.beam.runners.direct.portable;

import static com.google.common.collect.Iterables.getOnlyElement;

import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.ExecutableGraph;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

class FeedSDFEvaluatorFactory implements TransformEvaluatorFactory {
  static final String URN = "urn:beam:directrunner:transforms:feed_sdf:v1";

  private final BundleFactory bundleFactory;
  private final ExecutableGraph<PTransformNode, PCollectionNode> graph;

  FeedSDFEvaluatorFactory(
      ExecutableGraph<PTransformNode, PCollectionNode> graph, BundleFactory bundleFactory) {
    this.bundleFactory = bundleFactory;
    this.graph = graph;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      PTransformNode application, CommittedBundle<?> inputBundle) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    TransformEvaluator<InputT> evaluator =
        createEvaluator(application, (CommittedBundle) inputBundle);
    return evaluator;
  }

  @Override
  public void cleanup() {}

  private <InputT, RestrictionT> FeedSDFEvaluator<InputT, RestrictionT> createEvaluator(
      PTransformNode application,
      CommittedBundle<KeyedWorkItem<String, KV<InputT, RestrictionT>>> inputBundle) {
    @SuppressWarnings("unchecked")
    StructuralKey<String> key = (StructuralKey<String>) inputBundle.getKey();
    return new FeedSDFEvaluator<>(bundleFactory, key, application, graph);
  }

  private static class FeedSDFEvaluator<InputT, RestrictionT>
      implements TransformEvaluator<KeyedWorkItem<String, KV<InputT, RestrictionT>>> {
    private final BundleFactory bundleFactory;

    private final PTransformNode application;
    private final PCollectionNode outputCollection;

    private final StructuralKey<String> key;

    private final Collection<UncommittedBundle<?>> outputBundles;

    private FeedSDFEvaluator(
        BundleFactory bundleFactory,
        StructuralKey<String> key,
        PTransformNode application,
        ExecutableGraph<PTransformNode, PCollectionNode> graph) {
      this.bundleFactory = bundleFactory;
      this.application = application;
      this.outputCollection = getOnlyElement(graph.getProduced(application));
      this.key = key;
      outputBundles = new ArrayList<>();
    }

    @Override
    public void processElement(
        WindowedValue<KeyedWorkItem<String, KV<InputT, RestrictionT>>> windowedWorkItem)
        throws Exception {
      KeyedWorkItem<String, KV<InputT, RestrictionT>> workItem = windowedWorkItem.getValue();
      WindowedValue<KV<InputT, RestrictionT>> windowedValue =
          Iterables.getOnlyElement(workItem.elementsIterable());
      WindowedValue<InputT> element = windowedValue.withValue(windowedValue.getValue().getKey());
      KV<WindowedValue<InputT>, RestrictionT> elementAndRestriction =
          KV.of(element, windowedValue.getValue().getValue());

      UncommittedBundle<KV<InputT, RestrictionT>> bundle =
          bundleFactory.createKeyedBundle(this.key, outputCollection);
      bundle.add(
          elementAndRestriction
              .getKey()
              .withValue(
                  KV.of(
                      elementAndRestriction.getKey().getValue(),
                      elementAndRestriction.getValue())));
      outputBundles.add(bundle);
    }

    @Override
    public TransformResult<KeyedWorkItem<String, KV<InputT, RestrictionT>>> finishBundle()
        throws Exception {
      // State is initialized within the constructor. It can never be null.
      return StepTransformResult.<KeyedWorkItem<String, KV<InputT, RestrictionT>>>withoutHold(
              application)
          .addOutput(outputBundles)
          .build();
    }
  }
}
