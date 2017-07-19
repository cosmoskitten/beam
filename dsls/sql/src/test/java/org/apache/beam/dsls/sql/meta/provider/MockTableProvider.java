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
package org.apache.beam.dsls.sql.meta.provider;

import java.util.Collections;
import java.util.List;
import org.apache.beam.dsls.sql.meta.Table;
import org.apache.beam.dsls.sql.mock.MockedBoundedTable;
import org.apache.beam.dsls.sql.schema.BeamSqlRowType;
import org.apache.beam.dsls.sql.schema.BeamSqlTable;

/**
 * Mocked table provider.
 */
public class MockTableProvider  implements TableProvider {

  @Override public void init() {

  }

  @Override public String getTableType() {
    return "mock";
  }

  @Override public void createTable(Table table) {

  }

  @Override public List<Table> queryAllTables() {
    return Collections.emptyList();
  }

  @Override public BeamSqlTable buildBeamSqlTable(Table table) {
    BeamSqlRowType type = MetaUtils.getBeamSqlRecordTypeFromTable(table);
    return new MockedBoundedTable(type);
  }

  @Override public void close() {

  }
}
