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

import java.util.List;
import org.apache.beam.dsls.sql.meta.Table;
import org.apache.beam.dsls.sql.schema.BeamSqlTable;

/**
 * A {@code TableProvider} handles the metadata CRUD of a specified kind of tables.
 */
public interface TableProvider {
  /**
   * Init the provider.
   */
  void init();

  /**
   * Gets the table type this provider handles.
   */
  String getTableType();

  /**
   * Creates a table.
   */
  void createTable(Table table);

  /**
   * Query all tables from this provider.
   */
  List<Table> queryAllTables();

  /**
   * Build a {@link BeamSqlTable} using the given table meta info.
   */
  BeamSqlTable buildBeamSqlTable(Table table);

  /**
   * Close the provider.
   */
  void close();
}
