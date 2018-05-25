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
package org.apache.beam.sdk.nexmark;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.util.List;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Test;

/** Test class for BigQuery sinks. */
public class BigQuerySinkTest {

  private static final String NEXMARK_TABLE = "nexmark";
  private static final String NEXMARK_DATASET = "nexmark";
  private static final String GCP_PROJECT = "nexmark-test";
  private FakeDatasetService fakeDatasetService = new FakeDatasetService();
  private FakeJobService fakeJobService = new FakeJobService();
  private FakeBigQueryServices fakeBigQueryServices =
      new FakeBigQueryServices()
          .withDatasetService(fakeDatasetService)
          .withJobService(fakeJobService);

  @Before
  public void setUp() throws IOException, InterruptedException {
    FakeDatasetService.setUp();
    BigQueryIO.clearCreatedTables();
    fakeDatasetService.createDataset(GCP_PROJECT, NEXMARK_DATASET, "", "", null);
  }

  @Test
  public void testBigQueryWrite() throws Exception {
    NexmarkOptions options = PipelineOptionsFactory.create().as(NexmarkOptions.class);
    options.setBigQueryTable(NEXMARK_TABLE);
    options.setBigQueryDataset(NEXMARK_DATASET);
    options.setRunner(DirectRunner.class);
    options.setStreaming(true);
    options.setProject(GCP_PROJECT);
    options.setResourceNameMode(NexmarkUtils.ResourceNameMode.QUERY_RUNNER_AND_MODE);
    String queryName = "q0";
    NexmarkPerf nexmarkPerf1 = new NexmarkPerf();
    nexmarkPerf1.numResults = 1000L;
    nexmarkPerf1.eventsPerSec = 0.5F;
    nexmarkPerf1.runtimeSec = 0.3F;
    Main.writeQueryPerftoBigQuery(queryName, nexmarkPerf1, options, fakeBigQueryServices);
    NexmarkPerf nexmarkPerf2 = new NexmarkPerf();
    nexmarkPerf2.numResults = 1002L;
    nexmarkPerf2.eventsPerSec = 0.3F;
    nexmarkPerf2.runtimeSec = 0.2F;
    Main.writeQueryPerftoBigQuery(queryName, nexmarkPerf2, options, fakeBigQueryServices);
    // table should be created if not existing and rows appended to it
    String tableSpec = NexmarkUtils.tableSpec(options, queryName, 0L, null);
    assertNotNull(fakeDatasetService.getTable(BigQueryHelpers.parseTableSpec(tableSpec)));
    List<TableRow> actualRows =
        fakeDatasetService.getAllRows(
            options.getProject(),
            options.getBigQueryDataset(),
            BigQueryHelpers.parseTableSpec(tableSpec).getTableId());
    assertEquals("Wrong number of rows inserted", 2, actualRows.size());
    assertThat(actualRows,
        containsInAnyOrder(
            new TableRow()
                .set("Runtime(sec)", nexmarkPerf1.runtimeSec)
                .set("Events(/sec)", nexmarkPerf1.eventsPerSec)
                .set("Size of the result collection", nexmarkPerf1.numResults),
            new TableRow()
                .set("Runtime(sec)", nexmarkPerf2.runtimeSec)
                .set("Events(/sec)", nexmarkPerf2.eventsPerSec)
                .set("Size of the result collection", nexmarkPerf2.numResults)));
  }

}
