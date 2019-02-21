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
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.beam.runners.core.construction.expansion.ExpansionService;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesCrossLanguageTransforms;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.ConnectivityState;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.ManagedChannelBuilder;
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

  private static Server expansionServer;
  private static final int EXPANSION_PORT = 8096;
  private static final String EXPANSION_ADDR = String.format("localhost:%s", EXPANSION_PORT);
  private static String pythonServerCommand;

  @BeforeClass
  public static void setUp() throws IOException {
    pythonServerCommand = System.getProperty("pythonTestExpansionCommand");

    expansionServer =
        ServerBuilder.forPort(EXPANSION_PORT).addService(new ExpansionService()).build();
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
    PCollection<Integer> pcol =
        testPipeline
            .apply(Create.of(1, 2, 3))
            .apply("add one", External.of("simple", new byte[] {}, EXPANSION_ADDR))
            .apply(
                "filter <=3",
                External.of("le", "3".getBytes(StandardCharsets.UTF_8), EXPANSION_ADDR));

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

  private Process runCommandline(String command) {
    ProcessBuilder builder = new ProcessBuilder("sh", "-c", command);
    try {
      return builder.start();
    } catch (IOException e) {
      throw new AssertionError("process launch failed.");
    }
  }

  @Test
  @Category({ValidatesRunner.class, UsesCrossLanguageTransforms.class})
  public void expandPythonTest() {
    String target = "localhost:8095";
    Process p = runCommandline(pythonServerCommand);
    try {
      ManagedChannel channel = ManagedChannelBuilder.forTarget(target).build();
      ConnectivityState state = channel.getState(true);
      for (int retry = 0; retry < 5 && state != ConnectivityState.READY; retry++) {
        Thread.sleep(1000);
        state = channel.getState(true);
      }
      channel.shutdownNow();

      PCollection<String> pCol =
          testPipeline
              .apply(Create.of("1", "2", "2", "3", "3", "3"))
              .apply(
                  "toBytes",
                  MapElements.into(new TypeDescriptor<byte[]>() {}).via(String::getBytes))
              .apply(External.<byte[]>of("count_per_element_bytes", new byte[] {}, target))
              .apply("toString", MapElements.into(TypeDescriptors.strings()).via(String::new));

      PAssert.that(pCol).containsInAnyOrder("1->1", "2->2", "3->3");
      testPipeline.run();
    } catch (InterruptedException e) {
      throw new RuntimeException("interrupted.");
    } finally {
      p.destroyForcibly();
    }
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
