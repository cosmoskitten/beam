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
package org.apache.beam.sdk.nexmark.queries;

import java.util.Random;
import java.util.stream.Collectors;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test the various NEXMark queries yield results coherent with their models. */
@RunWith(JUnit4.class)
public class JoinToFilesTest {
  private static final NexmarkConfiguration config = NexmarkConfiguration.DEFAULT.copy();

  @Before
  public void setupConfig() {
    // careful, results of tests are linked to numEventGenerators because of timestamp generation
    config.numEventGenerators = 1;
    config.numEvents = 5000;

    // need side input data
    config.sideInputRowCount = 10;
    config.sideInputShards = 3;

    ResourceId sideInputResourceId = FileSystems.matchNewResource(
            String.format(
                    "%s/JoinToFiles-%s", p.getOptions().getTempLocation(), new Random().nextInt()), false);
    config.sideInputUrl = sideInputResourceId.toString();
  }

  @After
  public void cleanupSideInputUrl() throws Exception {
    // Technically, the test framework should own clearing up the temp location
    FileSystems.delete(
        FileSystems.match(config.sideInputUrl + "*").metadata().stream()
            .map(metadata -> metadata.resourceId())
            .collect(Collectors.toList()));
  }

  @Rule public TestPipeline p = TestPipeline.create();

  /** Test {@code query} matches {@code model}. */
  private void queryMatchesModel(
      String name, NexmarkQuery query, NexmarkQueryModel model, boolean streamingMode) {
    NexmarkUtils.setupPipeline(NexmarkUtils.CoderStrategy.HAND, p);
    NexmarkUtils.prepareSideInput(config);
    PCollection<TimestampedValue<KnownSize>> results;
    if (streamingMode) {
      results =
          p.apply(name + ".ReadUnBounded", NexmarkUtils.streamEventsSource(config)).apply(query);
    } else {
      results = p.apply(name + ".ReadBounded", NexmarkUtils.batchEventsSource(config)).apply(query);
    }
    PAssert.that(results).satisfies(model.assertionFor());
    PipelineResult result = p.run();
    result.waitUntilFinish();
  }

  @Test
  @Category(NeedsRunner.class)
  public void joinToFilesMatchesModelBatch() {
    queryMatchesModel(
        "JoinToFilesTestBatch", new JoinToFiles(config), new JoinToFilesModel(config), false);
  }

  @Test
  @Category(NeedsRunner.class)
  public void joinToFilesMatchesModelStreaming() {
    queryMatchesModel(
        "JoinToFilesTestStreaming", new JoinToFiles(config), new JoinToFilesModel(config), true);
  }
}
