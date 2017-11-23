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

import com.google.common.collect.ImmutableList;
import java.sql.Types;

/**
 * Builder class to construct {@link BeamRecordSqlType}.
 */
public class BeamRecordSqlTypeBuilder {

  private ImmutableList.Builder<String> fieldNames;
  private ImmutableList.Builder<Integer> fieldTypes;

  public BeamRecordSqlTypeBuilder withField(String fieldName, Integer fieldType) {
    fieldNames.add(fieldName);
    fieldTypes.add(fieldType);
    return this;
  }

  public BeamRecordSqlTypeBuilder withTinyIntField(String fieldName) {
    return withField(fieldName, Types.TINYINT);
  }

  public BeamRecordSqlTypeBuilder withSmallIntField(String fieldName) {
    return withField(fieldName, Types.SMALLINT);
  }

  public BeamRecordSqlTypeBuilder withIntegerField(String fieldName) {
    return withField(fieldName, Types.INTEGER);
  }

  public BeamRecordSqlTypeBuilder withBigIntField(String fieldName) {
    return withField(fieldName, Types.BIGINT);
  }

  public BeamRecordSqlTypeBuilder withFloatField(String fieldName) {
    return withField(fieldName, Types.FLOAT);
  }

  public BeamRecordSqlTypeBuilder withDoubleField(String fieldName) {
    return withField(fieldName, Types.DOUBLE);
  }

  public BeamRecordSqlTypeBuilder withDecimalField(String fieldName) {
    return withField(fieldName, Types.DECIMAL);
  }

  public BeamRecordSqlTypeBuilder withBooleanField(String fieldName) {
    return withField(fieldName, Types.BOOLEAN);
  }

  public BeamRecordSqlTypeBuilder withCharField(String fieldName) {
    return withField(fieldName, Types.CHAR);
  }

  public BeamRecordSqlTypeBuilder withVarcharField(String fieldName) {
    return withField(fieldName, Types.VARCHAR);
  }

  public BeamRecordSqlTypeBuilder withTimeField(String fieldName) {
    return withField(fieldName, Types.TIME);
  }

  public BeamRecordSqlTypeBuilder withDateField(String fieldName) {
    return withField(fieldName, Types.DATE);
  }

  public BeamRecordSqlTypeBuilder withTimestampField(String fieldName) {
    return withField(fieldName, Types.TIMESTAMP);
  }

  BeamRecordSqlTypeBuilder() {
    this.fieldNames = ImmutableList.builder();
    this.fieldTypes = ImmutableList.builder();
  }

  public static BeamRecordSqlTypeBuilder of() {
    return new BeamRecordSqlTypeBuilder();
  }

  public BeamRecordSqlType build() {
    return BeamRecordSqlType.create(fieldNames.build(), fieldTypes.build());
  }
}
