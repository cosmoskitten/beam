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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.base.Equivalence;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PipelineTranslation}. */
@RunWith(JUnit4.class)
public class PipelineTranslationTest {
  @Rule public TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void translatePipeline() {
    BigEndianLongCoder customCoder = BigEndianLongCoder.of();
    PCollection<Long> elems = pipeline.apply(GenerateSequence.from(0L).to(207L));
    PCollection<Long> counted = elems.apply(Count.<Long>globally()).setCoder(customCoder);
    PCollection<Long> windowed =
        counted.apply(
            Window.<Long>into(FixedWindows.of(Duration.standardMinutes(7)))
                .triggering(
                    AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings(AfterPane.elementCountAtLeast(19)))
                .accumulatingFiredPanes()
                .withAllowedLateness(Duration.standardMinutes(3L)));
    final WindowingStrategy<?, ?> windowedStrategy = windowed.getWindowingStrategy();
    PCollection<KV<String, Long>> keyed = windowed.apply(WithKeys.<String, Long>of("foo"));
    PCollection<KV<String, Iterable<Long>>> grouped =
        keyed.apply(GroupByKey.<String, Long>create());

    final RunnerApi.Pipeline pipelineProto = PipelineTranslation.translatePipeline(pipeline);
    pipeline.traverseTopologically(
        new PipelineProtoVerificationVisitor(pipelineProto));
  }

  @Test
  public void testTranslateThenRehydrate() throws Exception {
    pipeline.apply(Create.of(1, 2, 3));

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.translatePipeline(pipeline);
    Pipeline rehydrated = PipelineTranslation.rehydratePipeline(pipelineProto);

    rehydrated.traverseTopologically(
        new PipelineProtoVerificationVisitor(pipelineProto));
  }

  private static class PipelineProtoVerificationVisitor extends PipelineVisitor.Defaults {

    private final RunnerApi.Pipeline pipelineProto;
    Set<Node> transforms;
    Set<PCollection<?>> pcollections;
    Set<Equivalence.Wrapper<? extends Coder<?>>> coders;
    Set<WindowingStrategy<?, ?>> windowingStrategies;

    public PipelineProtoVerificationVisitor(RunnerApi.Pipeline pipelineProto) {
      this.pipelineProto = pipelineProto;
      transforms = new HashSet<>();
      pcollections = new HashSet<>();
      coders = new HashSet<>();
      windowingStrategies = new HashSet<>();
    }

    @Override
    public void leaveCompositeTransform(Node node) {
      if (node.isRootNode()) {
        assertThat(
            "Unexpected number of PTransforms",
            pipelineProto.getComponents().getTransformsCount(),
            equalTo(transforms.size()));
        assertThat(
            "Unexpected number of PCollections",
            pipelineProto.getComponents().getPcollectionsCount(),
            equalTo(pcollections.size()));
        assertThat(
            "Unexpected number of Coders",
            pipelineProto.getComponents().getCodersCount(),
            equalTo(coders.size()));
        assertThat(
            "Unexpected number of Windowing Strategies",
            pipelineProto.getComponents().getWindowingStrategiesCount(),
            equalTo(windowingStrategies.size()));
      } else {
        transforms.add(node);
      }
    }

    @Override
    public void visitPrimitiveTransform(Node node) {
      transforms.add(node);
    }

    @Override
    public void visitValue(PValue value, Node producer) {
      if (value instanceof PCollection) {
        PCollection pc = (PCollection) value;
        pcollections.add(pc);
        addCoders(pc.getCoder());
        windowingStrategies.add(pc.getWindowingStrategy());
        addCoders(pc.getWindowingStrategy().getWindowFn().windowCoder());
      }
    }

    private void addCoders(Coder<?> coder) {
      coders.add(Equivalence.<Coder<?>>identity().wrap(coder));
      if (coder instanceof StructuredCoder) {
        for (Coder<?> component : ((StructuredCoder<?>) coder).getComponents()) {
          addCoders(component);
        }
      }
    }
  }
}
