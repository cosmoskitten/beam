package org.apache.beam.dsls.sql.meta.provider.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.net.URI;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.dsls.sql.meta.Column;
import org.apache.beam.dsls.sql.meta.Table;
import org.apache.beam.dsls.sql.schema.BeamSqlTable;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for {@code KafkaTableProvider}.
 */
public class KafkaTableProviderTest {
  private static KafkaTableProvider provider;

  @BeforeClass
  public static void setUp() {
    provider = new KafkaTableProvider();
  }

  @Test public void buildBeamSqlTable() throws Exception {
    List<Column> columns = new ArrayList<>();
    columns.add(Column.builder().name("id").type(Types.VARCHAR).primaryKey(false).build());
    columns.add(Column.builder().name("name").type(Types.VARCHAR).primaryKey(false).build());
    columns.add(Column.builder().name("age").type(Types.INTEGER).primaryKey(false).build());

    JSONObject properties = new JSONObject();
    properties.put("bootstrap.servers", "localhost:9092");
    JSONArray topics = new JSONArray();
    topics.addAll(Arrays.asList("topic1", "topic2"));
    properties.put("topics", topics);

    Table table = Table.builder()
        .name("orders")
        .type("kafka")
        .columns(columns)
        .location(URI.create("kafka://localhost:2181/brokers?topic=test"))
        .properties(properties)
        .build();

    BeamSqlTable beamSqlTable = provider.buildBeamSqlTable(table);
    assertNotNull(beamSqlTable);
    assertTrue(beamSqlTable instanceof BeamKafkaCSVTable);
    BeamKafkaCSVTable kafkaCSVTable = (BeamKafkaCSVTable) beamSqlTable;
    assertEquals("localhost:9092",
        kafkaCSVTable.getBootstrapServers());
    assertEquals(Arrays.asList("topic1", "topic2"),
        kafkaCSVTable.getTopics());
  }
}
