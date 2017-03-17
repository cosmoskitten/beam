package org.apache.beam.sdk.io.gcp.bigquery;

import static com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import javax.annotation.Nullable;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Status;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * A set of helper functions and classes used by {@link BigQueryIO}.
 */
public class BigQueryHelpers {
  @Nullable
  /**
   * Return a displayable string representation for a {@link TableReference}.
   */
  static ValueProvider<String> displayTable(
      @Nullable ValueProvider<TableReference> table) {
    if (table == null) {
      return null;
    }
    return NestedValueProvider.of(table, new TableRefToTableSpec());
  }

  /**
   * Returns a canonical string representation of the {@link TableReference}.
   */
  static String toTableSpec(TableReference ref) {
    StringBuilder sb = new StringBuilder();
    if (ref.getProjectId() != null) {
      sb.append(ref.getProjectId());
      sb.append(":");
    }

    sb.append(ref.getDatasetId()).append('.').append(ref.getTableId());
    return sb.toString();
  }

  static <K, V> List<V> getOrCreateMapListValue(Map<K, List<V>> map, K key) {
    List<V> value = map.get(key);
    if (value == null) {
      value = new ArrayList<>();
      map.put(key, value);
    }
    return value;
  }

  /**
   * Parse a table specification in the form
   * {@code "[project_id]:[dataset_id].[table_id]"} or {@code "[dataset_id].[table_id]"}.
   *
   * <p>If the project id is omitted, the default project id is used.
   */
  static TableReference parseTableSpec(String tableSpec) {
    Matcher match = BigQueryIO.TABLE_SPEC.matcher(tableSpec);
    if (!match.matches()) {
      throw new IllegalArgumentException(
          "Table reference is not in [project_id]:[dataset_id].[table_id] "
          + "format: " + tableSpec);
    }

    TableReference ref = new TableReference();
    ref.setProjectId(match.group("PROJECT"));

    return ref.setDatasetId(match.group("DATASET")).setTableId(match.group("TABLE"));
  }

  static String jobToPrettyString(@Nullable Job job) throws IOException {
    return job == null ? "null" : job.toPrettyString();
  }

  static String statusToPrettyString(@Nullable JobStatus status) throws IOException {
    return status == null ? "Unknown status: null." : status.toPrettyString();
  }

  static Status parseStatus(@Nullable Job job) {
    if (job == null) {
      return Status.UNKNOWN;
    }
    JobStatus status = job.getStatus();
    if (status.getErrorResult() != null) {
      return Status.FAILED;
    } else if (status.getErrors() != null && !status.getErrors().isEmpty()) {
      return Status.FAILED;
    } else {
      return Status.SUCCEEDED;
    }
  }

  @VisibleForTesting
  static String toJsonString(Object item) {
    if (item == null) {
      return null;
    }
    try {
      return BigQueryIO.JSON_FACTORY.toString(item);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Cannot serialize %s to a JSON string.", item.getClass().getSimpleName()),
          e);
    }
  }

  @VisibleForTesting
  static <T> T fromJsonString(String json, Class<T> clazz) {
    if (json == null) {
      return null;
    }
    try {
      return BigQueryIO.JSON_FACTORY.fromString(json, clazz);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Cannot deserialize %s from a JSON string: %s.", clazz, json),
          e);
    }
  }

  /**
   * Returns a randomUUID string.
   *
   * <p>{@code '-'} is removed because BigQuery doesn't allow it in dataset id.
   */
  static String randomUUIDString() {
    return UUID.randomUUID().toString().replaceAll("-", "");
  }

  static void verifyTableNotExistOrEmpty(
      DatasetService datasetService,
      TableReference tableRef) {
    try {
      if (datasetService.getTable(tableRef) != null) {
        checkState(
            datasetService.isTableEmpty(tableRef),
            "BigQuery table is not empty: %s.",
            toTableSpec(tableRef));
      }
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException(
          "unable to confirm BigQuery table emptiness for table "
              + toTableSpec(tableRef), e);
    }
  }

  @VisibleForTesting
  static class JsonSchemaToTableSchema
      implements SerializableFunction<String, TableSchema> {
    @Override
    public TableSchema apply(String from) {
      return fromJsonString(from, TableSchema.class);
    }
  }

  @VisibleForTesting
  static class BeamJobUuidToBigQueryJobUuid
      implements SerializableFunction<String, String> {
    @Override
    public String apply(String from) {
      return "beam_job_" + from;
    }
  }

  static class TableSchemaToJsonSchema
      implements SerializableFunction<TableSchema, String> {
    @Override
    public String apply(TableSchema from) {
      return toJsonString(from);
    }
  }

  static class JsonTableRefToTableRef
      implements SerializableFunction<String, TableReference> {
    @Override
    public TableReference apply(String from) {
      return fromJsonString(from, TableReference.class);
    }
  }

  static class TableRefToTableSpec
      implements SerializableFunction<TableReference, String> {
    @Override
    public String apply(TableReference from) {
      return toTableSpec(from);
    }
  }

  static class TableRefToJson
      implements SerializableFunction<TableReference, String> {
    @Override
    public String apply(TableReference from) {
      return toJsonString(from);
    }
  }

  static class TableRefToProjectId
      implements SerializableFunction<TableReference, String> {
    @Override
    public String apply(TableReference from) {
      return from.getProjectId();
    }
  }

  @VisibleForTesting
  static class TableSpecToTableRef
      implements SerializableFunction<String, TableReference> {
    @Override
    public TableReference apply(String from) {
      return parseTableSpec(from);
    }
  }

  @VisibleForTesting
  static class CreatePerBeamJobUuid
      implements SerializableFunction<String, String> {
    private final String stepUuid;

    CreatePerBeamJobUuid(String stepUuid) {
      this.stepUuid = stepUuid;
    }

    @Override
    public String apply(String jobUuid) {
      return stepUuid + "_" + jobUuid.replaceAll("-", "");
    }
  }

  @VisibleForTesting
  static class CreateJsonTableRefFromUuid
      implements SerializableFunction<String, TableReference> {
    private final String executingProject;

    public CreateJsonTableRefFromUuid(String executingProject) {
      this.executingProject = executingProject;
    }

    @Override
    public TableReference apply(String jobUuid) {
      String queryTempDatasetId = "temp_dataset_" + jobUuid;
      String queryTempTableId = "temp_table_" + jobUuid;
      TableReference queryTempTableRef = new TableReference()
          .setProjectId(executingProject)
          .setDatasetId(queryTempDatasetId)
          .setTableId(queryTempTableId);
      return queryTempTableRef;
    }
  }
}
