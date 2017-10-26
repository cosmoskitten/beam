package org.apache.beam.sdk.extensions.sql.meta.provider.text;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.sql.Types;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Column;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.commons.csv.CSVFormat;
import org.junit.Test;

/**
 * UnitTest for {@link TextTableProvider}.
 */
public class TextTableProviderTest {
  private TextTableProvider provider = new TextTableProvider();

  @Test
  public void testGetTableType() throws Exception {
    assertEquals("text", provider.getTableType());
  }

  @Test
  public void testBuildBeamSqlTable() throws Exception {
    Table table = mockTable("hello", null);
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamTextCSVTable);

    BeamTextCSVTable csvTable = (BeamTextCSVTable) sqlTable;
    assertEquals(CSVFormat.DEFAULT, csvTable.getCsvFormat());
    assertEquals("/home/admin/hello", csvTable.getFilePattern());
  }

  @Test
  public void testBuildBeamSqlTable_customizedFormat() throws Exception {
    Table table = mockTable("hello", "Excel");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamTextCSVTable);

    BeamTextCSVTable csvTable = (BeamTextCSVTable) sqlTable;
    assertEquals(CSVFormat.EXCEL, csvTable.getCsvFormat());
  }

  private static Table mockTable(String name, String format) {
    JSONObject properties = new JSONObject();
    if (format != null) {
      properties.put("format", format);
    }
    return Table.builder()
        .name(name)
        .comment(name + " table")
        .location(URI.create("text://home/admin/" + name))
        .columns(ImmutableList.of(
            Column.builder().name("id").type(Types.INTEGER).primaryKey(true).build(),
            Column.builder().name("name").type(Types.VARCHAR).primaryKey(false).build()
        ))
        .type("text")
        .properties(properties)
        .build();
  }
}
