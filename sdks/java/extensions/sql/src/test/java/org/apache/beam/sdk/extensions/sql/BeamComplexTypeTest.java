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
package org.apache.beam.sdk.extensions.sql;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.mock.MockedBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;

/** Unit Tests for ComplexTypes, including nested ROW etc. */
public class BeamComplexTypeTest {
  private static final Schema innerRowSchema =
      Schema.builder().addStringField("one").addInt64Field("two").build();
  private static final Schema nestedRowSchema =
      Schema.builder()
          .addStringField("field1")
          .addRowField("RowField", innerRowSchema)
          .addInt64Field("field2")
          .addRowField("RowFieldTwo", innerRowSchema)
          .build();
  private static final Schema flattenedRowSchema =
      Schema.builder()
          .addStringField("field1")
          .addStringField("field2")
          .addInt64Field("field3")
          .addInt64Field("field4")
          .addStringField("field5")
          .addInt64Field("field6")
          .build();
  private static final Schema.FieldType nestedRowType = Schema.FieldType.row(nestedRowSchema);
  private static final ReadOnlyTableProvider readOnlyTableProvider =
      new ReadOnlyTableProvider(
          "test_provider",
          ImmutableMap.of(
              "rowTestTable",
              MockedBoundedTable.of(nestedRowType, "col")
                  .addRows(
                      Row.withSchema(nestedRowSchema)
                          .addValues(
                              "str",
                              Row.withSchema(innerRowSchema).addValues("inner_str_one", 1L).build(),
                              2L,
                              Row.withSchema(innerRowSchema).addValues("inner_str_two", 3L).build())
                          .build()),
              "singleRowTestTable",
              MockedBoundedTable.of(Schema.FieldType.row(innerRowSchema), "col")
                  .addRows(Row.withSchema(innerRowSchema).addValues("innerStr", 1L).build())));

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testNestedRow() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(readOnlyTableProvider);
    PCollection<Row> stream =
        BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery("SELECT col FROM rowTestTable"));
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(flattenedRowSchema)
                .addValues("str", "inner_str_one", 1L, 2L, "inner_str_two", 3L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testSingleRow() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(readOnlyTableProvider);
    PCollection<Row> stream =
        BeamSqlRelUtils.toPCollection(
            pipeline, sqlEnv.parseQuery("SELECT col FROM singleRowTestTable"));
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(innerRowSchema).addValues("innerStr", 1L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }
}
