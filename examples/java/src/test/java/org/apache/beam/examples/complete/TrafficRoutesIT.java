package org.apache.beam.examples.complete;

import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.QueryResponse;
import org.apache.beam.examples.complete.TrafficRoutes.FormatStatsFn;
import org.apache.beam.examples.complete.TrafficRoutes.TrafficRoutesOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryToTableIT.BigQueryToTableOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end tests of TrafficRoutes. */
@RunWith(JUnit4.class)
public class TrafficRoutesIT {
  private TrafficRoutesOptions options;
  private final String timestamp = Long.toString(System.currentTimeMillis());
  private final String outputDatasetId = "traffic_routes_" + timestamp;
  private final String outputTable = "traffic_routes_table";
  private String projectId;
  private BigqueryClient bqClient;

  @Before
  public void setupTestEnvironment() {
    PipelineOptionsFactory.register(BigQueryToTableOptions.class);
    this.options = TestPipeline.testingPipelineOptions().as(TrafficRoutesOptions.class);
    this.projectId = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    this.bqClient = new BigqueryClient("TrafficRoutesIT");
    this.bqClient.createNewDataset(this.projectId, this.outputDatasetId);
  }

  @After
  public void cleanupTestEnvironment() {
    this.bqClient.deleteDataset(this.projectId, this.outputDatasetId);
  }

  @Test
  public void testE2ETrafficRoutes() throws Exception {
    this.options.setBigQuerySchema(FormatStatsFn.getSchema());
    this.options.setProject(this.projectId);
    this.options.setBigQueryDataset(this.outputDatasetId);
    this.options.setBigQueryTable(this.outputTable);
    TrafficRoutes.runTrafficRoutes(options);

    QueryResponse response =
        this.bqClient.queryWithRetries(
            String.format(
                "SELECT count(*) as total FROM [%s:%s.%s]",
                this.projectId, this.outputDatasetId, this.outputTable),
            this.projectId);
    String res = response.getRows().get(0).getF().get(0).getV().toString();
    assertEquals("27", res);
  }
}
