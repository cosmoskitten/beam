package org.apache.beam.dsls.sql.meta.provider.text;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSONObject;
import java.net.URI;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.dsls.sql.meta.Column;
import org.apache.beam.dsls.sql.meta.Table;
import org.apache.beam.dsls.sql.schema.BeamSqlTable;
import org.apache.commons.csv.CSVFormat;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for {@link TextTableProvider}.
 */
public class TextTableProviderTest {
  private static TextTableProvider provider;

  @BeforeClass
  public static void setUp() {
    provider = new TextTableProvider();
  }

  @Test public void buildTable() throws Exception {
    List<Column> columns = new ArrayList<>();
    columns.add(Column.builder().name("id").type(Types.VARCHAR).primaryKey(false).build());
    columns.add(Column.builder().name("name").type(Types.VARCHAR).primaryKey(false).build());
    columns.add(Column.builder().name("age").type(Types.INTEGER).primaryKey(false).build());

    JSONObject properties = new JSONObject();
    properties.put("format", "Excel");
    Table table = Table.builder()
        .name("orders")
        .type("text")
        .columns(columns)
        .location(URI.create("text:///home/admin/person.txt"))
        .properties(properties)
        .build();

    BeamSqlTable beamSqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(beamSqlTable);
    assertTrue(beamSqlTable instanceof BeamTextCSVTable);
    BeamTextCSVTable textCSVTable = (BeamTextCSVTable) beamSqlTable;
    assertEquals("/home/admin/person.txt", textCSVTable.getFilePattern());
    assertEquals(CSVFormat.EXCEL, textCSVTable.getCsvFormat());
  }

  @Test public void buildTable_defaultCsvFormat() throws Exception {
    List<Column> columns = new ArrayList<>();
    columns.add(Column.builder().name("id").type(Types.VARCHAR).primaryKey(false).build());

    Table table = Table.builder()
        .name("orders")
        .type("text")
        .columns(columns)
        .location(URI.create("text:///home/admin/person.txt"))
        .properties(new JSONObject())
        .build();

    BeamSqlTable beamSqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(beamSqlTable);
    assertTrue(beamSqlTable instanceof BeamTextCSVTable);
    BeamTextCSVTable textCSVTable = (BeamTextCSVTable) beamSqlTable;
    assertEquals(CSVFormat.DEFAULT, textCSVTable.getCsvFormat());
  }
}
