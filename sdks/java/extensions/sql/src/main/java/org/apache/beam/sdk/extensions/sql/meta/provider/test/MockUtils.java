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
package org.apache.beam.sdk.extensions.sql.meta.provider.test;

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.apache.beam.sdk.values.Row.toRow;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.stream.Stream;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;

/** Utility functions for mock classes. */
@Experimental
public class MockUtils {
  public static Schema buildBeamSqlSchema(Object... args) {
    return Stream.iterate(0, i -> i + 2)
        .limit(args.length / 2)
        .map(i -> toRecordField(args, i))
        .collect(toSchema());
  }

  public static Schema.Field toRecordField(Object[] args, int i) {
    return Schema.Field.of((String) args[i + 1], (FieldType) args[i]);
  }

  public static List<Row> buildRows(Schema type, List<?> rowsValues) {
    return Lists.partition(rowsValues, type.getFieldCount())
        .stream()
        .map(values -> values.stream().collect(toRow(type)))
        .collect(toList());
  }
}
