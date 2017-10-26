package org.apache.beam.sdk.extensions.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.junit.Test;

/**
 * UnitTest for {@link BeamSqlCli}.
 */
public class BeamSqlCliTest {
  @Test
  public void testExecute_createTextTable() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli()
        .metaStore(metaStore)
        .defaultTableType("text");
    cli.execute(
        "create table person (\n"
        + "id int COMMENT 'id', \n"
        + "name varchar(31) COMMENT 'name', \n"
        + "age int COMMENT 'age') \n"
        + "COMMENT '' LOCATION 'text://home/admin/orders'"
    );
    Table table = metaStore.getTable("person");
    assertNotNull(table);
  }

  @Test
  public void testExplainQuery() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli()
        .metaStore(metaStore)
        .defaultTableType("text");
    cli.execute(
        "create table person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar(31) COMMENT 'name', \n"
            + "age int COMMENT 'age') \n"
            + "COMMENT '' LOCATION 'text://home/admin/orders'"
    );

    String plan = cli.explainQuery("select * from person");
    assertEquals(
        "BeamProjectRel(id=[$0], name=[$1], age=[$2])\n"
        + "  BeamIOSourceRel(table=[[person]])\n",
        plan
    );
  }
}
