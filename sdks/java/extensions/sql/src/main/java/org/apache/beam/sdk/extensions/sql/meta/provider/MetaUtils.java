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

package org.apache.beam.sdk.extensions.sql.meta.provider;

import static java.util.stream.Collectors.toList;

import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.sql.meta.Column;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.values.RowType;

/**
 * Utility methods for metadata.
 */
public class MetaUtils {
  public static RowType getRowTypeFromTable(Table table) {
    List<String> columnNames = table.getColumns().stream().map(Column::getName).collect(toList());
    List<Coder> columnTypes = table.getColumns().stream().map(Column::getCoder).collect(toList());
    return new RowType(columnNames, columnTypes);
  }
}
