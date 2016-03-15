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

package org.apache.beam.runners.gearpump.translators;

import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.runners.gearpump.GearpumpPipelineRunner;
import org.apache.beam.runners.gearpump.translators.utils.TestUtils;
import org.apache.beam.runners.gearpump.translators.utils.Verify;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import com.google.common.collect.Lists;

import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.cluster.embedded.EmbeddedCluster;
import org.junit.Test;

import java.util.List;

/**
 * integration test for {@link FlattenPCollectionTranslator}.
 */
public class FlattenPCollectionTranslatorTest {

  @Test
  public void test() throws Exception {
    GearpumpPipelineOptions options =
        PipelineOptionsFactory.as(GearpumpPipelineOptions.class);
    options.setApplicationName("FlattenPCollection");
    options.setRunner(GearpumpPipelineRunner.class);

    EmbeddedCluster cluster = EmbeddedCluster.apply();
    cluster.start();
    options.setEmbeddedCluster(cluster);

    Pipeline p = Pipeline.create(options);

    List<String> collection1 = Lists.newArrayList("1", "2", "3");
    List<String> collection2 = Lists.newArrayList("4", "5");
    List<String> expected = Lists.newArrayList("1", "2", "3", "4", "5");
    PCollection<String> pc1 =
        p.apply(Create.of(collection1).withCoder(StringUtf8Coder.of()));
    PCollection<String> pc2 =
        p.apply(Create.of(collection2).withCoder(StringUtf8Coder.of()));
    PCollectionList<String> pcs = PCollectionList.of(pc1).and(pc2);
    PCollection<String> actual = pcs.apply(Flatten.<String>pCollections());
    actual.apply(ParDo.of(new Verify<>(expected)));

    p.run();

    Thread.sleep(1000);
    ClientContext clientContext = options.getClientContext();
    TestUtils.checkFailure(clientContext);

    clientContext.close();
    cluster.stop();
  }
}
