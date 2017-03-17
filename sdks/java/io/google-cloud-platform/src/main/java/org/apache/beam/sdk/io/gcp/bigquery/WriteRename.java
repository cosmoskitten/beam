package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationTableCopy;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableReference;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Status;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copies temporary tables to destination table.
 */
class WriteRename extends DoFn<String, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(WriteRename.class);

  private final BigQueryServices bqServices;
  private final PCollectionView<String> jobIdToken;
  private final ValueProvider<String> jsonTableRef;
  private final WriteDisposition writeDisposition;
  private final CreateDisposition createDisposition;
  private final PCollectionView<Iterable<String>> tempTablesView;
  @Nullable
  private final String tableDescription;

  public WriteRename(
      BigQueryServices bqServices,
      PCollectionView<String> jobIdToken,
      ValueProvider<String> jsonTableRef,
      WriteDisposition writeDisposition,
      CreateDisposition createDisposition,
      PCollectionView<Iterable<String>> tempTablesView,
      @Nullable String tableDescription) {
    this.bqServices = bqServices;
    this.jobIdToken = jobIdToken;
    this.jsonTableRef = jsonTableRef;
    this.writeDisposition = writeDisposition;
    this.createDisposition = createDisposition;
    this.tempTablesView = tempTablesView;
    this.tableDescription = tableDescription;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    List<String> tempTablesJson = Lists.newArrayList(c.sideInput(tempTablesView));

    // Do not copy if no temp tables are provided
    if (tempTablesJson.size() == 0) {
      return;
    }

    List<TableReference> tempTables = Lists.newArrayList();
    for (String table : tempTablesJson) {
      tempTables.add(BigQueryHelpers.fromJsonString(table, TableReference.class));
    }
    copy(
        bqServices.getJobService(c.getPipelineOptions().as(BigQueryOptions.class)),
        bqServices.getDatasetService(c.getPipelineOptions().as(BigQueryOptions.class)),
        c.sideInput(jobIdToken),
        BigQueryHelpers.fromJsonString(jsonTableRef.get(), TableReference.class),
        tempTables,
        writeDisposition,
        createDisposition,
        tableDescription);

    DatasetService tableService =
        bqServices.getDatasetService(c.getPipelineOptions().as(BigQueryOptions.class));
    removeTemporaryTables(tableService, tempTables);
  }

  private void copy(
      JobService jobService,
      DatasetService datasetService,
      String jobIdPrefix,
      TableReference ref,
      List<TableReference> tempTables,
      WriteDisposition writeDisposition,
      CreateDisposition createDisposition,
      @Nullable String tableDescription) throws InterruptedException, IOException {
    JobConfigurationTableCopy copyConfig = new JobConfigurationTableCopy()
        .setSourceTables(tempTables)
        .setDestinationTable(ref)
        .setWriteDisposition(writeDisposition.name())
        .setCreateDisposition(createDisposition.name());

    String projectId = ref.getProjectId();
    Job lastFailedCopyJob = null;
    for (int i = 0; i < Write.MAX_RETRY_JOBS; ++i) {
      String jobId = jobIdPrefix + "-" + i;
      JobReference jobRef = new JobReference()
          .setProjectId(projectId)
          .setJobId(jobId);
      jobService.startCopyJob(jobRef, copyConfig);
      Job copyJob = jobService.pollJob(jobRef, Write.LOAD_JOB_POLL_MAX_RETRIES);
      Status jobStatus = BigQueryHelpers.parseStatus(copyJob);
      switch (jobStatus) {
        case SUCCEEDED:
          if (tableDescription != null) {
            datasetService.patchTableDescription(ref, tableDescription);
          }
          return;
        case UNKNOWN:
          throw new RuntimeException(String.format(
              "UNKNOWN status of copy job [%s]: %s.", jobId,
              BigQueryHelpers.jobToPrettyString(copyJob)));
        case FAILED:
          lastFailedCopyJob = copyJob;
          continue;
        default:
          throw new IllegalStateException(String.format(
              "Unexpected status [%s] of load job: %s.",
              jobStatus, BigQueryHelpers.jobToPrettyString(copyJob)));
      }
    }
    throw new RuntimeException(String.format(
        "Failed to create copy job with id prefix %s, "
            + "reached max retries: %d, last failed copy job: %s.",
        jobIdPrefix,
        Write.MAX_RETRY_JOBS,
        BigQueryHelpers.jobToPrettyString(lastFailedCopyJob)));
  }

  static void removeTemporaryTables(DatasetService tableService,
      List<TableReference> tempTables) {
    for (TableReference tableRef : tempTables) {
      try {
        LOG.debug("Deleting table {}", BigQueryHelpers.toJsonString(tableRef));
        tableService.deleteTable(tableRef);
      } catch (Exception e) {
        LOG.warn("Failed to delete the table {}", BigQueryHelpers.toJsonString(tableRef), e);
      }
    }
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);

    builder
        .addIfNotNull(DisplayData.item("jsonTableRef", jsonTableRef)
            .withLabel("Table Reference"))
        .add(DisplayData.item("writeDisposition", writeDisposition.toString())
            .withLabel("Write Disposition"))
        .add(DisplayData.item("createDisposition", createDisposition.toString())
            .withLabel("Create Disposition"));
  }
}
