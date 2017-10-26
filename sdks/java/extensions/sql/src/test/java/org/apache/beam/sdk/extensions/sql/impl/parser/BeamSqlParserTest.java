package org.apache.beam.sdk.extensions.sql.impl.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.sql.Types;
import org.apache.beam.sdk.extensions.sql.meta.Column;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

/**
 * UnitTest for {@link BeamSqlParser}.
 */
public class BeamSqlParserTest {
  @Test
  public void testParseCreateTable_full() throws Exception {
    JSONObject properties = new JSONObject();
    JSONArray hello = new JSONArray();
    hello.add("james");
    hello.add("bond");
    properties.put("hello", hello);

    Table table = parseTable(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar(31) COMMENT 'name') \n"
            + "COMMENT 'person table' \n"
            + "LOCATION 'text://home/admin/person'\n"
            + "TBLPROPERTIES '{\"hello\": [\"james\", \"bond\"]}'"
    );
    assertEquals(
        mockTable("person", "text", "person table", properties),
        table
    );
  }

  @Test
  public void testParseCreateTable_withoutTableComment() throws Exception {
    JSONObject properties = new JSONObject();
    JSONArray hello = new JSONArray();
    hello.add("james");
    hello.add("bond");
    properties.put("hello", hello);

    Table table = parseTable(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar(31) COMMENT 'name') \n"
            + "LOCATION 'text://home/admin/person'\n"
            + "TBLPROPERTIES '{\"hello\": [\"james\", \"bond\"]}'"
    );
    assertEquals(mockTable("person", "text", null, properties), table);
  }

  @Test
  public void testParseCreateTable_withoutTblProperties() throws Exception {
    Table table = parseTable(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar(31) COMMENT 'name') \n"
            + "COMMENT 'person table' \n"
            + "LOCATION 'text://home/admin/person'\n"
    );
    assertEquals(
        mockTable("person", "text", "person table", new JSONObject()),
        table
    );
  }

  @Test(expected = org.apache.beam.sdk.extensions.sql.impl.parser.impl.ParseException.class)
  public void testParseCreateTable_withoutLocation() throws Exception {
    parseTable(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar(31) COMMENT 'name') \n"
            + "COMMENT 'person table' \n"
    );
  }

  private Table parseTable(String sql) throws Exception {
    BeamSqlParser parser = new BeamSqlParser(sql);
    SqlNode sqlNode = parser.impl().parseSqlStmtEof();

    assertNotNull(sqlNode);
    assertTrue(sqlNode instanceof SqlCreateTable);
    SqlCreateTable stmt = (SqlCreateTable) sqlNode;
    return ParserUtils.convertCreateTableStmtToTable(stmt);
  }

  private static Table mockTable(String name, String type, String comment, JSONObject properties) {
    return Table.builder()
        .name(name)
        .type(type)
        .comment(comment)
        .location(URI.create("text://home/admin/" + name))
        .columns(ImmutableList.of(
            Column.builder()
                .name("id")
                .type(Types.INTEGER)
                .primaryKey(false)
                .comment("id")
                .build(),
            Column.builder()
                .name("name")
                .type(Types.VARCHAR)
                .primaryKey(false)
                .comment("name")
                .build()
        ))
        .properties(properties)
        .build();
  }
}
