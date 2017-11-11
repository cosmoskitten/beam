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

import com.google.common.collect.ImmutableList;

import java.sql.Types;

import org.apache.beam.sdk.extensions.sql.BeamRecordSqlType;

/**
 * Helper class to construct {@link BeamRecordSqlType}.
 */
public class BeamRecordSqlTypeBuilder {

  private ImmutableList.Builder<String> fieldNames;
  private ImmutableList.Builder<Integer> fieldTypes;

  BeamRecordSqlTypeBuilder withField(String fieldName, Integer fieldType) {
    fieldNames.add(fieldName);
    fieldTypes.add(fieldType);
    return this;
  }

  BeamRecordSqlTypeBuilder withLongField(String fieldName) {
    return withField(fieldName, Types.BIGINT);
  }

  BeamRecordSqlTypeBuilder withStringField(String fieldName) {
    return withField(fieldName, Types.VARCHAR);
  }

  private BeamRecordSqlTypeBuilder() {
    this.fieldNames = ImmutableList.builder();
    this.fieldTypes = ImmutableList.builder();
  }

  public static BeamRecordSqlTypeBuilder of() {
    return new BeamRecordSqlTypeBuilder();
  }

  BeamRecordSqlType build() {
    return BeamRecordSqlType.create(fieldNames.build(), fieldTypes.build());
  }
}
