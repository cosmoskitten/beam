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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.Nullable;

/**
 * Inserts rows into BigQuery.
 */
class BigQueryUtil {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryUtil.class);

  private final DatasetService datasetService;

  /**
   * Constructs a new row inserter.
   *
   * @param datasetService the {@link DatasetService}.
   */
  BigQueryUtil(DatasetService datasetService) {
    this.datasetService = datasetService;
  }

  /**
   * Retrieves or creates the table.
   *
   * <p>The table is checked to conform to insertion requirements as specified
   * by WriteDisposition and CreateDisposition.
   *
   * <p>If table truncation is requested (WriteDisposition.WRITE_TRUNCATE), then
   * this will re-create the table if necessary to ensure it is empty.
   *
   * <p>If an empty table is required (WriteDisposition.WRITE_EMPTY), then this
   * will fail if the table exists and is not empty.
   *
   * <p>When constructing a table, a {@code TableSchema} must be available.  If a
   * schema is provided, then it will be used.  If no schema is provided, but
   * an existing table is being cleared (WRITE_TRUNCATE option above), then
   * the existing schema will be re-used.  If no schema is available, then an
   * {@code IOException} is thrown.
   */
  void getOrCreateTable(
      TableReference ref,
      WriteDisposition writeDisposition,
      CreateDisposition createDisposition,
      @Nullable TableSchema schema) throws IOException, InterruptedException {
    // Check if table already exists.
    Table table = datasetService.getTable(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());

    if (table == null) {
      if (createDisposition == CreateDisposition.CREATE_IF_NEEDED) {
        checkNotNull(schema, "Table schema required for new table.");
        datasetService.createTable(new Table().setTableReference(ref).setSchema(schema));
      } else {
        throw new IllegalStateException(String.format(
            "Table is not found: %s, and CreateDisposition is not CREATE_IF_NEEDED", ref));
      }
    } else {
      if (writeDisposition == WriteDisposition.WRITE_APPEND) {
        return;
      }
      boolean empty =
          datasetService.isTableEmpty(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
      if (writeDisposition == WriteDisposition.WRITE_EMPTY && empty) {
        return;
      } else if (writeDisposition == WriteDisposition.WRITE_EMPTY && !empty) {
        throw new IOException(
            String.format("WriteDisposition is WRITE_EMPTY, but table: %s is not empty", ref));
      } else if (writeDisposition == WriteDisposition.WRITE_TRUNCATE && empty) {
        LOG.info("Empty table found, not removing {}", BigQueryIO.toTableSpec(ref));
        return;
      } else if (writeDisposition == WriteDisposition.WRITE_EMPTY && empty) {
        return;
      } else if (writeDisposition == WriteDisposition.WRITE_TRUNCATE && !empty) {
        // Reuse the existing schema if none was provided.
        if (schema == null) {
          schema = table.getSchema();
        }
        checkNotNull(schema, "Table schema required for new table.");
        // Delete table and fall through to re-creating it below.
        LOG.info("Deleting table {}", BigQueryIO.toTableSpec(ref));
        datasetService.deleteTable(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
      }
    }
  }
}
