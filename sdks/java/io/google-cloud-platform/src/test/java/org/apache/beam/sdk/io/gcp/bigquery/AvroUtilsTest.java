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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link AvroUtils}. */
@RunWith(JUnit4.class)
public class AvroUtilsTest {
  @Test
  public void testSubMilliPrecisionRejected() {
    assertThrows(
        "precision",
        IllegalArgumentException.class,
        () ->
            AvroUtils.convertAvroFormat(
                Schema.Field.of("dummy", Schema.FieldType.DATETIME), 1000000001L));
  }

  @Test
  public void testMilliPrecisionOk() {
    long millis = 123456789L;
    assertThat(
        AvroUtils.convertAvroFormat(
            Schema.Field.of("dummy", Schema.FieldType.DATETIME), millis * 1000),
        equalTo(new Instant(millis)));
  }

  @Test
  public void testSubMilliPrecisionLogicalTypeRejected() {
    assertThrows(
        "precision",
        IllegalArgumentException.class,
        () ->
            AvroUtils.convertAvroFormat(
                Schema.Field.of("dummy", Schema.FieldType.logicalType(new FakeSqlTimeType())),
                1000000001L));
  }

  @Test
  public void testMilliPrecisionOkLogicaltype() {
    long millis = 123456789L;
    assertThat(
        AvroUtils.convertAvroFormat(
            Schema.Field.of("dummy", Schema.FieldType.logicalType(new FakeSqlTimeType())),
            millis * 1000),
        equalTo(new Instant(millis)));
  }

  private static class FakeSqlTimeType implements Schema.LogicalType<Long, Instant> {
    @Override
    public String getIdentifier() {
      return "SqlTimeType";
    }

    @Override
    public Schema.FieldType getBaseType() {
      return Schema.FieldType.DATETIME;
    }

    @Override
    public Instant toBaseType(Long input) {
      // Already converted to millis outside this constructor
      return new Instant((long) input);
    }

    @Override
    public Long toInputType(Instant base) {
      return base.getMillis();
    }
  }
}
