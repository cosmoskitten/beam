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
package org.apache.beam.runners.dataflow;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects.firstNonNull;

import com.google.api.client.util.ArrayMap;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricUpdate;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricFiltering;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation of {@link MetricResults} for the Dataflow Runner. */
class DataflowMetrics extends MetricResults {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowMetrics.class);
  /**
   * Client for the Dataflow service. This can be used to query the service for information about
   * the job.
   */
  private DataflowClient dataflowClient;

  /**
   * PipelineResult implementation for Dataflow Runner. It contains job state and id information.
   */
  private DataflowPipelineJob dataflowPipelineJob;

  /**
   * After the job has finished running, Metrics no longer will change, so their results are cached
   * here.
   */
  private JobMetrics cachedMetricResults = null;

  /**
   * Constructor for the DataflowMetrics class.
   *
   * @param dataflowPipelineJob is used to get Job state and Job ID information.
   * @param dataflowClient is used to query user metrics from the Dataflow service.
   */
  public DataflowMetrics(DataflowPipelineJob dataflowPipelineJob, DataflowClient dataflowClient) {
    this.dataflowClient = dataflowClient;
    this.dataflowPipelineJob = dataflowPipelineJob;
  }

  /**
   * Take a list of metric updates coming from the Dataflow service, and format it into a Metrics
   * API MetricQueryResults instance.
   *
   * @param metricUpdates
   * @return a populated MetricQueryResults object.
   */
  private MetricQueryResults populateMetricQueryResults(
      List<MetricUpdate> metricUpdates, MetricsFilter filter) {
    return DataflowMetricQueryResultsFactory.create(dataflowPipelineJob, metricUpdates, filter)
        .build();
  }

  @Override
  public MetricQueryResults queryMetrics(@Nullable MetricsFilter filter) {
    List<MetricUpdate> metricUpdates;
    ImmutableList<MetricResult<Long>> counters = ImmutableList.of();
    ImmutableList<MetricResult<DistributionResult>> distributions = ImmutableList.of();
    ImmutableList<MetricResult<GaugeResult>> gauges = ImmutableList.of();
    JobMetrics jobMetrics;
    try {
      jobMetrics = getJobMetrics();
    } catch (IOException e) {
      LOG.warn("Unable to query job metrics.\n");
      return MetricQueryResults.create(counters, distributions, gauges);
    }
    metricUpdates = firstNonNull(jobMetrics.getMetrics(), Collections.<MetricUpdate>emptyList());
    return populateMetricQueryResults(metricUpdates, filter);
  }

  private JobMetrics getJobMetrics() throws IOException {
    if (cachedMetricResults != null) {
      // Metric results have been cached after the job ran.
      return cachedMetricResults;
    }
    JobMetrics result = dataflowClient.getJobMetrics(dataflowPipelineJob.jobId);
    if (dataflowPipelineJob.getState().isTerminal()) {
      // Add current query result to the cache.
      cachedMetricResults = result;
    }
    return result;
  }

  private static class DataflowMetricResultExtractor {
    private final ImmutableList.Builder<MetricResult<Long>> counterResults;
    private final ImmutableList.Builder<MetricResult<DistributionResult>> distributionResults;
    private final ImmutableList.Builder<MetricResult<GaugeResult>> gaugeResults;
    private final boolean isStreamingJob;

    DataflowMetricResultExtractor(boolean isStreamingJob) {
      counterResults = ImmutableList.builder();
      distributionResults = ImmutableList.builder();
      gaugeResults = ImmutableList.builder();
      this.isStreamingJob = isStreamingJob;
    }

    public void addMetricResult(
        MetricKey metricKey, @Nullable MetricUpdate committed, @Nullable MetricUpdate attempted) {
      if (committed == null || attempted == null) {
        LOG.warn(
            "Metric {} did not have both a committed ({}) and tentative value ({}).",
            metricKey,
            committed,
            attempted);
      } else if (committed.getDistribution() != null && attempted.getDistribution() != null) {
        // distribution metric
        DistributionResult value = getDistributionValue(committed);
        distributionResults.add(
            MetricResult.create(
                metricKey,
                isStreamingJob ? null : value, // Committed
                value)); // Attempted
        /* In Dataflow streaming jobs, only ATTEMPTED metrics are available.
         * In Dataflow batch jobs, only COMMITTED metrics are available, but
         * we must provide ATTEMPTED, so we use COMMITTED as a good approximation.
         * Reporting the appropriate metric depending on whether it's a batch/streaming job.
         */
      } else if (committed.getScalar() != null && attempted.getScalar() != null) {
        // counter metric
        Long value = getCounterValue(committed);
        counterResults.add(
            MetricResult.create(
                metricKey,
                isStreamingJob ? null : value, // Committed
                value)); // Attempted
        /* In Dataflow streaming jobs, only ATTEMPTED metrics are available.
         * In Dataflow batch jobs, only COMMITTED metrics are available, but
         * we must provide ATTEMPTED, so we use COMMITTED as a good approximation.
         * Reporting the appropriate metric depending on whether it's a batch/streaming job.
         */
      } else {
        // This is exceptionally unexpected. We expect matching user metrics to only have the
        // value types provided by the Metrics API.
        LOG.warn(
            "Unexpected / mismatched metric types."
                + " Please report JOB ID to Dataflow Support. Metric key: {}."
                + " Committed / attempted Metric updates: {} / {}",
            metricKey.toString(),
            committed.toString(),
            attempted.toString());
      }
    }

    private Long getCounterValue(MetricUpdate metricUpdate) {
      if (metricUpdate.getScalar() == null) {
        return 0L;
      }
      return ((Number) metricUpdate.getScalar()).longValue();
    }

    private DistributionResult getDistributionValue(MetricUpdate metricUpdate) {
      if (metricUpdate.getDistribution() == null) {
        return DistributionResult.IDENTITY_ELEMENT;
      }
      ArrayMap distributionMap = (ArrayMap) metricUpdate.getDistribution();
      Long count = ((Number) distributionMap.get("count")).longValue();
      Long min = ((Number) distributionMap.get("min")).longValue();
      Long max = ((Number) distributionMap.get("max")).longValue();
      Long sum = ((Number) distributionMap.get("sum")).longValue();
      return DistributionResult.create(sum, count, min, max);
    }

    public Iterable<MetricResult<DistributionResult>> getDistributionResults() {
      return distributionResults.build();
    }

    public Iterable<MetricResult<Long>> getCounterResults() {
      return counterResults.build();
    }

    public Iterable<MetricResult<GaugeResult>> getGaugeResults() {
      return gaugeResults.build();
    }
  }

  private static class DataflowMetricQueryResultsFactory {
    private final Iterable<MetricUpdate> metricUpdates;
    private final MetricsFilter filter;
    private final HashMap<MetricKey, MetricUpdate> tentativeByName;
    private final HashMap<MetricKey, MetricUpdate> committedByName;
    private final HashSet<MetricKey> metricHashKeys;
    private final DataflowPipelineJob dataflowPipelineJob;

    public static DataflowMetricQueryResultsFactory create(
        DataflowPipelineJob dataflowPipelineJob,
        Iterable<MetricUpdate> metricUpdates,
        MetricsFilter filter) {
      return new DataflowMetricQueryResultsFactory(dataflowPipelineJob, metricUpdates, filter);
    }

    private DataflowMetricQueryResultsFactory(
        DataflowPipelineJob dataflowPipelineJob,
        Iterable<MetricUpdate> metricUpdates,
        MetricsFilter filter) {
      this.dataflowPipelineJob = dataflowPipelineJob;
      this.metricUpdates = metricUpdates;
      this.filter = filter;

      tentativeByName = new HashMap<>();
      committedByName = new HashMap<>();
      metricHashKeys = new HashSet<>();
    }

    /**
     * Check whether a {@link MetricUpdate} is a tentative update or not.
     *
     * @return true if update is tentative, false otherwise
     */
    private boolean isMetricTentative(MetricUpdate metricUpdate) {
      return metricUpdate.getName().getContext().containsKey("tentative")
          && Objects.equal(metricUpdate.getName().getContext().get("tentative"), "true");
    }

    /**
     * Build an {@link MetricKey} that serves as a hash key for a metric update.
     *
     * @return a {@link MetricKey} that can be hashed and used to identify a metric.
     */
    private MetricKey getMetricHashKey(MetricUpdate metricUpdate) {
      String fullStepName = metricUpdate.getName().getContext().get("step");
      if (dataflowPipelineJob.transformStepNames == null
          || !dataflowPipelineJob.transformStepNames.inverse().containsKey(fullStepName)) {
        // If we can't translate internal step names to user step names, we just skip them
        // altogether.
        return null;
      }
      fullStepName =
          dataflowPipelineJob.transformStepNames.inverse().get(fullStepName).getFullName();
      return MetricKey.ptransform(
          fullStepName,
          metricUpdate.getName().getContext().get("namespace"),
          metricUpdate.getName().getName());
    }

    private void buildMetricsIndex() {
      // If the Context of the metric update does not have a namespace, then these are not
      // actual metrics counters.
      for (MetricUpdate update : metricUpdates) {
        if (update.getName().getOrigin() != null
            && (!"user".equalsIgnoreCase(update.getName().getOrigin())
                || !update.getName().getContext().containsKey("namespace"))) {
          // Skip non-user metrics, which should have both a "user" origin and a namespace.
          continue;
        }

        MetricKey updateKey = getMetricHashKey(update);
        if (updateKey == null || !MetricFiltering.matches(filter, updateKey)) {
          // Skip unmatched metrics early.
          continue;
        }

        metricHashKeys.add(updateKey);
        if (isMetricTentative(update)) {
          MetricUpdate previousUpdate = tentativeByName.put(updateKey, update);
          if (previousUpdate != null) {
            LOG.warn("Metric {} already had a tentative value of {}", updateKey, previousUpdate);
          }
        } else {
          MetricUpdate previousUpdate = committedByName.put(updateKey, update);
          if (previousUpdate != null) {
            LOG.warn("Metric {} already had a committed value of {}", updateKey, previousUpdate);
          }
        }
      }
    }

    public MetricQueryResults build() {
      buildMetricsIndex();

      DataflowMetricResultExtractor extractor =
          new DataflowMetricResultExtractor(dataflowPipelineJob.getDataflowOptions().isStreaming());
      for (MetricKey metricKey : metricHashKeys) {
        String metricName = metricKey.metricName().getName();
        if (metricName.endsWith("[MIN]")
            || metricName.endsWith("[MAX]")
            || metricName.endsWith("[MEAN]")
            || metricName.endsWith("[COUNT]")) {
          // Skip distribution metrics, as these are not yet properly supported.
          // TODO: remove this when distributions stop being broken up for the UI.
          continue;
        }

        extractor.addMetricResult(
            metricKey, committedByName.get(metricKey), tentativeByName.get(metricKey));
      }
      return MetricQueryResults.create(
          extractor.getCounterResults(),
          extractor.getDistributionResults(),
          extractor.getGaugeResults());
    }
  }
}
