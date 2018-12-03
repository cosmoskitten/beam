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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.model.construction.v1.ConstructionApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Impulse;
import org.junit.Test;

/** Tests for {@link ConstructionService}. */
public class ConstructionServiceTest {

  private static final String TEST_URN = "test:beam:transforms:count";

  private static final String TEST_NAME = "TestName";

  private static final String TEST_NAMESPACE = "namespace";

  private ConstructionService constructionService = new ConstructionService();

  /** Registers a single test transformation. */
  @AutoService(ConstructionService.ConstructionServiceRegistrar.class)
  public static class TestTransforms implements ConstructionService.ConstructionServiceRegistrar {
    @Override
    public Map<String, ConstructionService.TransformProvider> knownTransforms() {
      return ImmutableMap.of(TEST_URN, spec -> Count.perElement());
    }
  }

  @Test
  public void testConstruct() {
    Pipeline p = Pipeline.create();
    p.apply(Impulse.create());
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    String inputPcollId =
        Iterables.getOnlyElement(
            Iterables.getOnlyElement(pipelineProto.getComponents().getTransformsMap().values())
                .getOutputsMap()
                .values());
    ConstructionApi.ConstructionRequest request =
        ConstructionApi.ConstructionRequest.newBuilder()
            .setComponents(pipelineProto.getComponents())
            .setTransform(
                RunnerApi.PTransform.newBuilder()
                    .setUniqueName(TEST_NAME)
                    .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(TEST_URN))
                    .putInputs("input", inputPcollId))
            .setNamespace(TEST_NAMESPACE)
            .build();
    ConstructionApi.ConstructionResponse response = constructionService.construct(request);
    RunnerApi.PTransform expandedTransform =
        response.getComponents().getTransformsOrThrow(response.getTransformId());
    assertEquals(TEST_NAME, expandedTransform.getUniqueName());
    // Verify it has the right input.
    assertEquals(inputPcollId, Iterables.getOnlyElement(expandedTransform.getInputsMap().values()));
    // Loose check that it's composite, and its children are represented.
    assertNotEquals(expandedTransform.getSubtransformsCount(), 0);
    for (String subtransform : expandedTransform.getSubtransformsList()) {
      assertTrue(response.getComponents().containsTransforms(subtransform));
    }
    // Check that any newly generated components are properly namespaced.
    Set<String> originalIds = allIds(request.getComponents());
    for (String id : allIds(response.getComponents())) {
      assertTrue(id, id.startsWith(TEST_NAMESPACE) || originalIds.contains(id));
    }
  }

  public Set<String> allIds(RunnerApi.Components components) {
    Set<String> all = new HashSet<>();
    all.addAll(components.getTransformsMap().keySet());
    all.addAll(components.getPcollectionsMap().keySet());
    all.addAll(components.getCodersMap().keySet());
    all.addAll(components.getWindowingStrategiesMap().keySet());
    all.addAll(components.getEnvironmentsMap().keySet());
    return all;
  }
}
