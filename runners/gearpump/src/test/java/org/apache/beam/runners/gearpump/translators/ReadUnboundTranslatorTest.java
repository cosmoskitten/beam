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
import org.apache.beam.runners.gearpump.translators.utils.CollectionSource;
import org.apache.beam.runners.gearpump.translators.utils.TestUtils;
import org.apache.beam.runners.gearpump.translators.utils.Verify;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

import com.google.common.collect.Lists;

import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.cluster.embedded.EmbeddedCluster;
import org.junit.Test;

import java.util.List;

/**
 * integration test for {@link ReadUnboundedTranslator}.
 */
public class ReadUnboundTranslatorTest {

  @Test
  public void test() throws Exception {
    GearpumpPipelineOptions options = PipelineOptionsFactory.create()
        .as(GearpumpPipelineOptions.class);
    options.setApplicationName("ReadUnbound");
    options.setRunner(GearpumpPipelineRunner.class);

    EmbeddedCluster cluster = EmbeddedCluster.apply();
    cluster.start();
    options.setEmbeddedCluster(cluster);

    Pipeline pipeline = Pipeline.create(options);

    List<String> collection = Lists.newArrayList("1", "2", "3", "4", "5");
    CollectionSource<String> source = new CollectionSource<>(collection, StringUtf8Coder.of());
    pipeline.apply(Read.from(source))
        .apply(ParDo.of(new Verify<>(collection)));

    pipeline.run();

    Thread.sleep(1000);
    ClientContext clientContext = options.getClientContext();
    TestUtils.checkFailure(clientContext);

    clientContext.close();
    cluster.stop();
  }

}
