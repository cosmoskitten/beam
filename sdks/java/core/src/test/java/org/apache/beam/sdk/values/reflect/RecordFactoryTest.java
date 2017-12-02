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

package org.apache.beam.sdk.values.reflect;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.values.BeamRecord;
import org.junit.Test;

/**
 * Unit tests for {@link RecordFactoryTest}.
 */
public class RecordFactoryTest {

  /**
   * Test pojo.
   */
  public static final class SomePojo {
    private String someStringField;
    private Integer someIntegerField;

    public Boolean publicBooleanField;

    public SomePojo(String someStringField, Integer someIntegerField) {
      this.someStringField = someStringField;
      this.someIntegerField = someIntegerField;
    }

    public String getSomeStringField() {
      return someStringField;
    }

    public Integer getSomeIntegerField() {
      return someIntegerField;
    }
  }

  @Test
  public void testNewRecordFieldValues() throws Exception {
    SomePojo pojo = new SomePojo("someString", 42);
    pojo.publicBooleanField = true;
    RecordFactory factory = new RecordFactory();

    BeamRecord record = factory.newRecordCopyOf(pojo);

    assertEquals(3, record.getFieldCount());
    assertThat(
        record.getDataValues(),
        containsInAnyOrder(
            (Object) "someString", Integer.valueOf(42), Boolean.TRUE));
  }

  @Test
  public void testNewRecordFieldNames() throws Exception {
    SomePojo pojo = new SomePojo("someString", 42);
    pojo.publicBooleanField = true;
    RecordFactory factory = new RecordFactory();

    BeamRecord record = factory.newRecordCopyOf(pojo);

    assertThat(record.getDataType().getFieldNames(),
        containsInAnyOrder(
            "publicBooleanField", "someStringField", "someIntegerField"));
  }

  @Test
  public void testCreatesNewInstanceEachTime() throws Exception {
    SomePojo pojo = new SomePojo("someString", 42);
    pojo.publicBooleanField = true;
    RecordFactory factory = new RecordFactory();

    BeamRecord record1 = factory.newRecordCopyOf(pojo);
    BeamRecord record2 = factory.newRecordCopyOf(pojo);

    assertNotSame(record1, record2);
  }

  @Test
  public void testCachesRecordType() throws Exception {
    SomePojo pojo = new SomePojo("someString", 42);
    pojo.publicBooleanField = true;
    RecordFactory factory = new RecordFactory();

    BeamRecord record1 = factory.newRecordCopyOf(pojo);
    BeamRecord record2 = factory.newRecordCopyOf(pojo);

    assertSame(record1.getDataType(), record2.getDataType());
  }

  @Test
  public void testCopiesValues() throws Exception {
    SomePojo pojo = new SomePojo("someString", 42);
    pojo.publicBooleanField = true;
    RecordFactory factory = new RecordFactory();

    BeamRecord record = factory.newRecordCopyOf(pojo);

    assertThat(
        record.getDataValues(),
        containsInAnyOrder(
            (Object) "someString", Integer.valueOf(42), Boolean.TRUE));

    pojo.someIntegerField = 23;
    pojo.someStringField = "hello";
    pojo.publicBooleanField = false;

    assertThat(
        record.getDataValues(),
        containsInAnyOrder(
            (Object) "someString", Integer.valueOf(42), Boolean.TRUE));
  }
}
