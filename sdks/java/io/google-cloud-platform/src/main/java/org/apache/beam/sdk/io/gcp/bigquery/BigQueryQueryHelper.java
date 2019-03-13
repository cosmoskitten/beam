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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createJobIdToken;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.createTempTableReference;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.EncryptionConfiguration;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.TableReference;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.Status;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.JobService;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper object for executing query jobs in BigQuery.
 *
 * <p>This object is not serializable, and its state can be safely discarded across serialization
 * boundaries for any associated source objects.
 */
class BigQueryQueryHelper {

  private static final Integer JOB_POLL_MAX_RETRIES = Integer.MAX_VALUE;

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryQueryHelper.class);

  private final String stepUuid;
  private final ValueProvider<String> queryProvider;
  private final Boolean flattenResults;
  private final Boolean useLegacySql;
  private final QueryPriority priority;
  private final String location;
  private final String kmsKey;
  private final BigQueryServices bqServices;

  private AtomicReference<JobStatistics> dryRunJobStats;

  public BigQueryQueryHelper(
      String stepUuid,
      ValueProvider<String> queryProvider,
      Boolean flattenResults,
      Boolean useLegacySql,
      QueryPriority priority,
      @Nullable String location,
      @Nullable String kmsKey,
      BigQueryServices bqServices) {
    this.stepUuid = checkNotNull(stepUuid, "stepUuid");
    this.queryProvider = checkNotNull(queryProvider, "queryProvider");
    this.flattenResults = checkNotNull(flattenResults, "flattenResults");
    this.useLegacySql = checkNotNull(useLegacySql, "useLegacySql");
    this.priority = checkNotNull(priority, "priority");
    this.location = location;
    this.kmsKey = kmsKey;
    this.bqServices = checkNotNull(bqServices, "bqServices");
    this.dryRunJobStats = new AtomicReference<>();
  }

  public synchronized JobStatistics dryRunQueryIfNeeded(BigQueryOptions options)
      throws InterruptedException, IOException {
    if (dryRunJobStats.get() == null) {
      JobStatistics jobStatistics =
          bqServices
              .getJobService(options)
              .dryRunQuery(options.getProject(), createBasicQueryConfig(), location);
      dryRunJobStats.compareAndSet(null, jobStatistics);
    }
    return dryRunJobStats.get();
  }

  public TableReference executeQuery(BigQueryOptions options)
      throws InterruptedException, IOException {
    // Step 1: Find the effective location of the query.
    String effectiveLocation = location;
    DatasetService tableService = bqServices.getDatasetService(options);
    if (effectiveLocation == null) {
      List<TableReference> referencedTables =
          dryRunQueryIfNeeded(options).getQuery().getReferencedTables();
      if (referencedTables != null && !referencedTables.isEmpty()) {
        TableReference referencedTable = referencedTables.get(0);
        effectiveLocation = tableService.getTable(referencedTable).getLocation();
      }
    }

    // Step 2: Create a temporary dataset in the query location.
    String jobIdToken = createJobIdToken(options.getJobName(), stepUuid);
    TableReference queryResultTable = createTempTableReference(options.getProject(), jobIdToken);
    LOG.info("Creating temporary dataset {} for query results", queryResultTable.getDatasetId());

    tableService.createDataset(
        queryResultTable.getProjectId(),
        queryResultTable.getDatasetId(),
        effectiveLocation,
        "Temporary tables for query results of job " + options.getJobName(),
        TimeUnit.DAYS.toMillis(1));

    // Step 3: Execute the query. Generate a transient (random) query job ID, because this code may
    // be retried after the temporary dataset and table have been deleted by a previous attempt --
    // in that case, we want to regenerate the temporary dataset and table, and we'll need a fresh
    // query ID to do that.
    String queryJobId = jobIdToken + "-query-" + BigQueryHelpers.randomUUIDString();
    LOG.info(
        "Exporting query results into temporary table {} using job {}",
        queryResultTable,
        queryJobId);

    JobReference jobReference =
        new JobReference()
            .setProjectId(options.getProject())
            .setLocation(effectiveLocation)
            .setJobId(queryJobId);

    JobConfigurationQuery queryConfiguration =
        createBasicQueryConfig()
            .setAllowLargeResults(true)
            .setDestinationTable(queryResultTable)
            .setCreateDisposition("CREATE_IF_NEEDED")
            .setWriteDisposition("WRITE_TRUNCATE")
            .setPriority(priority.name());

    if (kmsKey != null) {
      queryConfiguration.setDestinationEncryptionConfiguration(
          new EncryptionConfiguration().setKmsKeyName(kmsKey));
    }

    JobService jobService = bqServices.getJobService(options);
    jobService.startQueryJob(jobReference, queryConfiguration);
    Job job = jobService.pollJob(jobReference, JOB_POLL_MAX_RETRIES);
    if (BigQueryHelpers.parseStatus(job) != Status.SUCCEEDED) {
      throw new IOException(
          String.format(
              "Query job %s failed, status: %s",
              queryJobId, BigQueryHelpers.statusToPrettyString(job.getStatus())));
    }

    LOG.info("Query job {} completed", queryJobId);
    return queryResultTable;
  }

  private JobConfigurationQuery createBasicQueryConfig() {
    return new JobConfigurationQuery()
        .setQuery(queryProvider.get())
        .setFlattenResults(flattenResults)
        .setUseLegacySql(useLegacySql);
  }
}
