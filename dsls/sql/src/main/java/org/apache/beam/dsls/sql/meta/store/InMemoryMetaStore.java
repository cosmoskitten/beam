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

package org.apache.beam.dsls.sql.meta.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.dsls.sql.meta.Table;
import org.apache.beam.dsls.sql.meta.provider.TableProvider;
import org.apache.beam.dsls.sql.schema.BeamSqlTable;

/**
 * A {@link MetaStore} which stores the meta info in memory.
 *
 * <p>NOTE, because this implementation is memory based, the metadata is NOT persistent.
 * for tables which created, you need to create again every you launch the
 * {@link org.apache.beam.dsls.sql.BeamSqlCli}.
 */
public class InMemoryMetaStore implements MetaStore {
  private Map<String, Table> tables = new HashMap<>();
  private Map<String, TableProvider> providers = new HashMap<>();

  public InMemoryMetaStore() {
    // init the providers
    ServiceLoader<TableProvider> loader = ServiceLoader.load(TableProvider.class);
    for (TableProvider provider : loader) {
      provider.init();
      providers.put(provider.getTableType(), provider);
    }

    // Init the tables, The tables from all providers should be unique. if there is any duplication,
    // the init process will fail.
    for (TableProvider provider : providers.values()) {
      initTablesFromProvider(provider);
    }
  }

  private void initTablesFromProvider(TableProvider provider) {
    List<Table> tables = provider.queryAllTables();
    for (Table table : tables) {
      if (this.tables.containsKey(table.getName())) {
        throw new IllegalStateException(
            "Duplicate table: " + table.getName() + " from provider: " + provider);
      }

      this.tables.put(table.getName(), table);
    }
  }

  @Override public void createTable(Table table) {
    validateTableType(table);

    // first assert the table name is unique
    if (tables.containsKey(table.getName())) {
      throw new IllegalArgumentException("Duplicate table name: " + table.getName());
    }

    // invoke the provider's create
    providers.get(table.getType()).createTable(table);

    // store to the global metastore
    tables.put(table.getName(), table);
  }

  @Override public Table queryTable(String tableName) {
    return tables.get(tableName);
  }

  @Override public List<Table> queryAllTables() {
    return new ArrayList<>(tables.values());
  }

  @Override public BeamSqlTable buildBeamSqlTable(String tableName) {
    Table table = queryTable(tableName);

    if (table == null) {
      throw new IllegalArgumentException("The specified table: " + tableName + " does not exists!");
    }

    TableProvider provider = providers.get(table.getType());

    return provider.buildBeamSqlTable(table);
  }

  private void validateTableType(Table table) {
    if (!providers.containsKey(table.getType())) {
      throw new UnsupportedOperationException(
          "Table type: " + table.getType() + " not supported!");
    }
  }

  public void registerProvider(TableProvider provider) {
    this.providers.put(provider.getTableType(), provider);
    initTablesFromProvider(provider);
  }
}
