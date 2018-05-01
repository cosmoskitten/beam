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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;

/**
 * A {@code BeamSqlTableProvider} provides read only set of {@code BeamSqlTable}.
 */
public class BeamSqlTableProvider implements TableProvider {
  private final String typeName;
  private final Map<String, BeamSqlTable> tables;

  public BeamSqlTableProvider(String typeName, Map<String, BeamSqlTable> tables) {
    this.typeName = typeName;
    this.tables = tables;
  }

  @Override public String getTableType() {
    return typeName;
  }

  @Override
  public void createTable(Table table) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropTable(String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Table> listTables() {
    List<Table> list = new ArrayList(tables.size());
    for (Map.Entry<String, BeamSqlTable> table : tables.entrySet()) {
      list.add(Table.builder()
          .type(getTableType())
          .name(table.getKey())
          .columns(Collections.emptyList())
          .build());
    }
    return list;
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return tables.get(table.getName());
  }
}
