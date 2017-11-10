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

package org.apache.beam.sdk.nexmark.model.sql.adapter;

import static org.junit.Assert.assertEquals;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.extensions.sql.BeamRecordSqlType;

import org.junit.Test;

/**
 * Unit tests for {@link BeamRecordSqlTypeBuilder}.
 */
public class BeamRecordSqlTypeBuilderTest {

  private static final List<String> FIELD_NAMES = Arrays.asList("f1", "f2", "f3");
  private static final List<Integer> FIELD_TYPES =
      Arrays.asList(Types.INTEGER, Types.BIGINT, Types.TIME);

  @Test
  public void testBuildsRecordType() throws Exception {
    BeamRecordSqlType recordType = BeamRecordSqlTypeBuilder.of()
        .withField("f1", Types.INTEGER)
        .withField("f2", Types.BIGINT)
        .withField("f3", Types.TIME)
        .build();

    assertEquals(FIELD_NAMES, recordType.getFieldNames());
    assertEquals(FIELD_TYPES, recordType.getFieldTypes());
  }
}
