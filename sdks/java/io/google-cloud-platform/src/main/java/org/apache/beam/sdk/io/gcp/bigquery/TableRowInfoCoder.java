package org.apache.beam.sdk.io.gcp.bigquery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TableRowJsonCoder;

/**
 * Created by relax on 3/17/17.
 */
@VisibleForTesting
class TableRowInfoCoder extends AtomicCoder<TableRowInfo> {
  private static final TableRowInfoCoder INSTANCE = new TableRowInfoCoder();

  @JsonCreator
  public static TableRowInfoCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(TableRowInfo value, OutputStream outStream, Context context)
      throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null value");
    }
    tableRowCoder.encode(value.tableRow, outStream, context.nested());
    idCoder.encode(value.uniqueId, outStream, context);
  }

  @Override
  public TableRowInfo decode(InputStream inStream, Context context)
      throws IOException {
    return new TableRowInfo(
        tableRowCoder.decode(inStream, context.nested()),
        idCoder.decode(inStream, context));
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this, "TableRows are not deterministic.");
  }

  TableRowJsonCoder tableRowCoder = TableRowJsonCoder.of();
  StringUtf8Coder idCoder = StringUtf8Coder.of();
}
