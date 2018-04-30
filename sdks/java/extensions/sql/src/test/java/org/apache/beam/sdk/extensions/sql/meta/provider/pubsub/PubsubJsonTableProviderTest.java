/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsub;

import static junit.framework.TestCase.assertNotNull;
import static org.apache.beam.sdk.extensions.sql.RowSqlTypes.INTEGER;
import static org.apache.beam.sdk.extensions.sql.RowSqlTypes.VARCHAR;
import static org.junit.Assert.assertEquals;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Column;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link PubsubJsonTableProvider}.
 */
public class PubsubJsonTableProviderTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testTableTypePubsub() {
    PubsubJsonTableProvider provider = new PubsubJsonTableProvider();
    assertEquals("pubsub", provider.getTableType());
  }

  @Test
  public void testCreatesTable() {
    PubsubJsonTableProvider provider = new PubsubJsonTableProvider();

    BeamSqlTable pubsubTable = provider.buildBeamSqlTable(fakeTable());
    assertNotNull(pubsubTable);
  }


  @Test
  public void testThrowsIfTimestampAttributeNotProvided() {
    PubsubJsonTableProvider provider = new PubsubJsonTableProvider();

    Table table = fakeTable().toBuilder().properties(new JSONObject()).build();

    thrown.expectMessage("timestampAttributeKey");
    provider.buildBeamSqlTable(table);
  }

  @Test
  public void testThrowsIfTimestampFieldExistsInSchema() {
    PubsubJsonTableProvider provider = new PubsubJsonTableProvider();

    Table table =
        fakeTable()
            .toBuilder()
            .columns(
                Collections.singletonList(
                    Column
                        .builder()
                        .name("ts_field")
                        .fieldType(VARCHAR)
                        .nullable(true)
                        .build()))
            .build();

    thrown.expectMessage("ts_field");
    provider.buildBeamSqlTable(table);
  }

  private static Table fakeTable() {
    return
        Table
            .builder()
            .name("FakeTable")
            .comment("fake table")
            .location("projects/project/topics/topic")
            .columns(
                ImmutableList.of(
                    Column
                        .builder()
                        .name("id")
                        .fieldType(INTEGER)
                        .nullable(true)
                        .build(),
                    Column
                        .builder()
                        .name("name")
                        .fieldType(VARCHAR)
                        .nullable(true)
                        .build()
                ))
            .type("pubsub")
            .properties(JSON.parseObject("{ \"timestampAttributeKey\" : \"ts_field\" }"))
            .build();
  }
}
