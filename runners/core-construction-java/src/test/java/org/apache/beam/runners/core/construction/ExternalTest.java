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

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesCrossLanguageTransforms;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.ServerBuilder;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test External transforms. */
@RunWith(JUnit4.class)
public class ExternalTest implements Serializable {
  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  public static Server expansionServer;

  @BeforeClass
  public static void setUp() throws IOException {
    expansionServer = ServerBuilder.forPort(8097).addService(new ExpansionService()).build();
    expansionServer.start();
  }

  @AfterClass
  public static void tearDown() {
    expansionServer.shutdownNow();
  }

  @Test
  @Category({ValidatesRunner.class, UsesCrossLanguageTransforms.class})
  public void expandTest() {
    POutput col =
        testPipeline
            .apply(Create.of(1, 2, 3))
            .apply(External.of("simple", new byte[] {}, "localhost:8097"));
    PAssert.that((PCollection<Integer>) col).containsInAnyOrder(2, 3, 4);
    testPipeline.run();
  }

  /** Test TransformProvider. */
  @AutoService(ExpansionService.ExpansionServiceRegistrar.class)
  public static class TestTransforms implements ExpansionService.ExpansionServiceRegistrar {
    @Override
    public Map<String, ExpansionService.TransformProvider> knownTransforms() {
      return ImmutableMap.of(
          "simple", spec -> MapElements.into(TypeDescriptors.integers()).via((Integer x) -> x + 1));
    }
  }
}
