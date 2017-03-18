package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableRow;

/**
 * Created by relax on 3/17/17.
 */
class TableRowInfo {
  TableRowInfo(TableRow tableRow, String uniqueId) {
    this.tableRow = tableRow;
    this.uniqueId = uniqueId;
  }

  final TableRow tableRow;
  final String uniqueId;
}
