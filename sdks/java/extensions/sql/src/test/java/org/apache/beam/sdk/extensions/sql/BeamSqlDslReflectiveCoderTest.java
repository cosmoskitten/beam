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

import java.io.Serializable;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests for GROUP-BY/aggregation, with global_window/fix_time_window/sliding_window/session_window
 * with BOUNDED PCollection.
 */
public class BeamSqlDslReflectiveCoderTest extends BeamSqlDslBase {

  @Rule public final TestPipeline pipeline = TestPipeline.create();


  public static class PersonPojo implements Serializable {
    private Integer ageYears;
    private String name;

    public Integer getAgeYears() {
      return ageYears;
    }

    public String getName() {
      return name;
    }

    PersonPojo(String name, Integer ageYears) {
      this.ageYears = ageYears;
      this.name = name;
    }
  }

  @Test
  public void testMagic() {
    PCollection<PersonPojo> input =
        PBegin.in(pipeline).apply(
            "input",
            Create
                .of(
                    new PersonPojo("Foo", 5),
                    new PersonPojo("Bar", 53))
                .withCoder(
                    InferredSqlRowCoder.ofSerializable(PersonPojo.class)));

    String sql =
        "SELECT * FROM PCOLLECTION";

    PCollection<Row> result = input.apply("sql", BeamSql.query(sql));

    PAssert
        .that(result)
        .containsInAnyOrder(
            TestUtils
                .rowsBuilderOf(
                    RowSqlType
                        .builder()
                        .withVarcharField("name")
                        .withIntegerField("ageYears")
                        .build())
                .addRows(
                    "Foo", 5,
                    "Bar", 53)
                .getRows());

    pipeline.run();
  }
}
