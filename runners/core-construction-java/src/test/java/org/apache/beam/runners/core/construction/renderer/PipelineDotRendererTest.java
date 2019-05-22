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
package org.apache.beam.runners.core.construction.renderer;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PipelineDotRenderer}. */
@RunWith(JUnit4.class)
public class PipelineDotRendererTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void testEmptyPipeline() {
    assertEquals(
        "digraph {\n"
            + "    rankdir=LR\n"
            + "    subgraph cluster_0 {\n"
            + "        label = \"\"\n"
            + "    }\n"
            + "}\n",
        PipelineDotRenderer.toDotString(p));
  }

  @Test
  public void testCompositePipeline() {
    p.apply(Create.timestamped(TimestampedValue.of(KV.of(1, 1), new Instant(1))))
        .apply(Window.into(FixedWindows.of(Duration.millis(10))))
        .apply(Sum.integersPerKey());

    assertEquals(
        "digraph {\n"
            + "    rankdir=LR\n"
            + "    subgraph cluster_0 {\n"
            + "        label = \"\"\n"
            + "        subgraph cluster_1 {\n"
            + "            label = \"Create.TimestampedValues\"\n"
            + "            subgraph cluster_2 {\n"
            + "                label = \"Create.TimestampedValues/Create.Values\"\n"
            + "                3 [label=\"Read(CreateSource)\"]\n"
            + "            }\n"
            + "            subgraph cluster_4 {\n"
            + "                label = \"Create.TimestampedValues/ParDo(ConvertTimestamps)\"\n"
            + "                5 [label=\"ParMultiDo(ConvertTimestamps)\"]\n"
            + "                3 -> 5 [style=solid label=\"402#bb20b45fd4d95138\"]\n"
            + "            }\n"
            + "        }\n"
            + "        subgraph cluster_6 {\n"
            + "            label = \"Window.Into()\"\n"
            + "            7 [label=\"Window.Assign\"]\n"
            + "            5 -> 7 [style=solid label=\"402#3d93cb799b3970be\"]\n"
            + "        }\n"
            + "        subgraph cluster_8 {\n"
            + "            label = \"Combine.perKey(SumInteger)\"\n"
            + "            9 [label=\"GroupByKey\"]\n"
            + "            7 -> 9 [style=solid label=\"402#a32dc9f64f1df03a\"]\n"
            + "            subgraph cluster_10 {\n"
            + "                label = \"Combine.perKey(SumInteger)/Combine.GroupedValues\"\n"
            + "                subgraph cluster_11 {\n"
            + "                    label = \"Combine.perKey(SumInteger)/Combine.GroupedValues/ParDo(Anonymous)\"\n"
            + "                    12 [label=\"ParMultiDo(Anonymous)\"]\n"
            + "                    9 -> 12 [style=solid label=\"402#8ce970b71df42503\"]\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    }\n"
            + "}\n",
        PipelineDotRenderer.toDotString(p));
  }
}
