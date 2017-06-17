package org.apache.beam.dsls.sql.meta.provider;

import java.util.Collections;
import java.util.List;
import org.apache.beam.dsls.sql.meta.Table;
import org.apache.beam.dsls.sql.planner.MockedBeamSqlTable;
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlTable;

/**
 * Mocked table provider.
 */
public class MockTableProvider  implements TableProvider {

  @Override public void init() {

  }

  @Override public String getTableType() {
    return "mock";
  }

  @Override public void createTable(Table table) {

  }

  @Override public List<Table> queryAllTables() {
    return Collections.emptyList();
  }

  @Override public BeamSqlTable buildBeamSqlTable(Table table) {
    BeamSqlRecordType type = MetaUtils.getBeamSqlRecordTypeFromTable(table);
    return new MockedBeamSqlTable(type);
  }

  @Override public void close() {

  }
}
