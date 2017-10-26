package org.apache.beam.sdk.extensions.sql.impl.parser;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.Column;
import org.apache.beam.sdk.extensions.sql.meta.Table;

/**
 * Util method for parser.
 */
public class ParserUtils {

  /**
   * Convert a create table statement to a {@code Table} object.
   * @param stmt
   * @return the table
   */
  public static Table convertCreateTableStmtToTable(SqlCreateTable stmt) {
    List<Column> columns = new ArrayList<>(stmt.fieldList().size());
    for (ColumnDefinition columnDef : stmt.fieldList()) {
      Column column = Column.builder()
          .name(columnDef.name().toLowerCase())
          .type(
              CalciteUtils.toJavaType(
                  columnDef.type().deriveType(BeamQueryPlanner.TYPE_FACTORY).getSqlTypeName()
              )
          )
          .comment(columnDef.comment())
          .primaryKey(columnDef.constraint() instanceof ColumnConstraint.PrimaryKey)
          .build();
      columns.add(column);
    }

    String tableType = stmt.location() == null
        ? null : stmt.location().getScheme();

    Table table = Table.builder()
        .type(tableType)
        .name(stmt.tableName().toLowerCase())
        .columns(columns)
        .comment(stmt.comment())
        .location(stmt.location())
        .properties(stmt.properties())
        .build();

    return table;
  }
}
