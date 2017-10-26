package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.sql.Types;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Column;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.junit.Test;

/**
 * UnitTest for {@link KafkaTableProvider}.
 */
public class KafkaTableProviderTest {
  private KafkaTableProvider provider = new KafkaTableProvider();
  @Test public void testBuildBeamSqlTable() throws Exception {
    Table table = mockTable("hello");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof BeamKafkaCSVTable);

    BeamKafkaCSVTable csvTable = (BeamKafkaCSVTable) sqlTable;
    assertEquals("localhost:9092", csvTable.getBootstrapServers());
    assertEquals(ImmutableList.of("topic1", "topic2"), csvTable.getTopics());
  }

  @Test
  public void testGetTableType() throws Exception {
    assertEquals("kafka", provider.getTableType());
  }

  private static Table mockTable(String name) {
    JSONObject properties = new JSONObject();
    properties.put("bootstrap.servers", "localhost:9092");
    JSONArray topics = new JSONArray();
    topics.add("topic1");
    topics.add("topic2");
    properties.put("topics", topics);

    return Table.builder()
        .name(name)
        .comment(name + " table")
        .location(URI.create("kafka://localhost:2181/brokers?topic=test"))
        .columns(ImmutableList.of(
            Column.builder().name("id").type(Types.INTEGER).primaryKey(true).build(),
            Column.builder().name("name").type(Types.VARCHAR).primaryKey(false).build()
        ))
        .type("kafka")
        .properties(properties)
        .build();
  }
}
