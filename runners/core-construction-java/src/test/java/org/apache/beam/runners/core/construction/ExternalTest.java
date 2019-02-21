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
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.beam.runners.core.construction.expansion.ExpansionService;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesCrossLanguageTransforms;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.ServerBuilder;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test External transforms. */
@RunWith(JUnit4.class)
public class ExternalTest implements Serializable {
  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  private static Server expansionServer;
  private static int EXPANSION_PORT = 8096;
  private static String EXPANSION_ADDR = String.format("localhost:%s", EXPANSION_PORT);

  @BeforeClass
  public static void setUp() throws IOException {
    expansionServer = ServerBuilder.forPort(EXPANSION_PORT).addService(new ExpansionService()).build();
    expansionServer.start();
  }

  @AfterClass
  public static void tearDown() {
    expansionServer.shutdownNow();
  }

  @Test
  @Category({ValidatesRunner.class, UsesCrossLanguageTransforms.class})
  public void expandSingleTest() {
    PCollection<Integer> col =
        testPipeline
            .apply(Create.of(1, 2, 3))
            .apply(External.of("simple", new byte[] {}, EXPANSION_ADDR));
    PAssert.that(col).containsInAnyOrder(2, 3, 4);
    testPipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class, UsesCrossLanguageTransforms.class})
  public void expandMultipleTest() {
    byte[] three = new byte[] {};
    try {
      three = "3".getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      Assert.fail(e.getMessage());
    }

    PCollection<Integer> pcol =
        testPipeline
            .apply(Create.of(1, 2, 3))
            .apply("add one", External.of("simple", new byte[] {}, EXPANSION_ADDR))
            .apply("filter <=3", External.of("le", three, EXPANSION_ADDR));

    PAssert.that(pcol).containsInAnyOrder(2, 3);
    testPipeline.run();
  }

  @Test
  @Category({ValidatesRunner.class, UsesCrossLanguageTransforms.class})
  public void expandMultiOutputTest() {
    PCollectionTuple pTuple =
        testPipeline
            .apply(Create.of(1, 2, 3, 4, 5, 6))
            .apply(External.of("multi", new byte[] {}, EXPANSION_ADDR).withMultiOutputs());

    PAssert.that(pTuple.get(new TupleTag<Integer>("aTag") {})).containsInAnyOrder(2, 4, 6);
    PAssert.that(pTuple.get(new TupleTag<Integer>("bTag") {})).containsInAnyOrder(1, 3, 5);
    testPipeline.run();
  }

  //@Test
  @Category({ValidatesRunner.class, UsesCrossLanguageTransforms.class})
  public void expandPythonTest() {
    PCollection<String> pCol =
            testPipeline
                    .apply(Create.of("1", "2", "2", "3", "3", "3"))
                    .apply("toBytes", MapElements.into(new TypeDescriptor<byte[]>(){}).via(String::getBytes))
                    .apply(External.<byte[]>of("count_per_element_bytes", new byte[] {}, "localhost:8095"))
                    .apply("toString", MapElements.into(TypeDescriptors.strings()).via(String::new));

    PAssert.that(pCol).containsInAnyOrder("1->1", "2->2", "3->3");
    testPipeline.run();
  }

  /** Test TransformProvider. */
  @AutoService(ExpansionService.ExpansionServiceRegistrar.class)
  public static class TestTransforms
      implements ExpansionService.ExpansionServiceRegistrar, Serializable {
    private final TupleTag<Integer> aTag = new TupleTag<Integer>("aTag") {};
    private final TupleTag<Integer> bTag = new TupleTag<Integer>("bTag") {};

    @Override
    public Map<String, ExpansionService.TransformProvider> knownTransforms() {
      return ImmutableMap.of(
          "simple", spec -> MapElements.into(TypeDescriptors.integers()).via((Integer x) -> x + 1),
          "le", spec -> Filter.lessThanEq(Integer.parseInt(spec.getPayload().toStringUtf8())),
          "multi",
              spec ->
                  ParDo.of(
                          new DoFn<Integer, Integer>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                              if (c.element() % 2 == 0) {
                                c.output(c.element());
                              } else {
                                c.output(bTag, c.element());
                              }
                            }
                          })
                      .withOutputTags(aTag, TupleTagList.of(bTag)));
    }
  }
}
