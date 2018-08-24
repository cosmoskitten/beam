package org.apache.beam.sdk.extensions.sql.impl.parser;

import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

/** Throws SQLFeatureNotSupportedException for {@code CREATE TABLE} statement. */
public class SqlCreateTableNotSupportedMessage extends SqlCreate {
  public SqlCreateTableNotSupportedMessage(SqlParserPos pos, boolean replace)
      throws ParseException {
    super(pos, replace);
    throw new ParseException(
        new SQLFeatureNotSupportedException(
            "'CREATE TABLE' is not supported in BeamSQL. You can use 'CREATE EXTERNAL TABLE' to "
                + "register an external data source to BeamSQL"));
  }

  @Override
  public List<SqlNode> getOperandList() {
    return null;
  }
}
