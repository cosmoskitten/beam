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
package org.apache.beam.sdk.extensions.sql.impl;

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.extensions.sql.meta.store.MetaStore;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;

/**
 * Factory classes that Calcite uses to create initial schema for JDBC connection.
 *
 * <p>Calcite creates a factory using default constructor and isn't flexible enough to hook into the
 * process to pass in additional parameters (e.g. connection), so we have to add the indirection
 * layer.
 *
 * <p>{@link LoadAllProviders} is the default factory that greedily finds and loads all available
 * table providers. This is used in a normal JDBC path, e.g. when CLI connects to {@link
 * JdbcDriver}.
 *
 * <p>{@link EmptySchema} is used by {@link JdbcDriver#connect(TableProvider)} to avoid loading all
 * available table providers.
 */
public class BeamCalciteSchemaFactory {

  /**
   * Called by {@link JdbcConnection} when initializing to convert the initial empty schema to
   * actual {@link BeamCalciteSchema}.
   */
  static TableProvider fromInitialEmptySchema(JdbcConnection jdbcConnection) {
    InitialEmptySchema initialEmptySchema = jdbcConnection.getCurrentBeamSchema();
    return initialEmptySchema.toTableProvider(jdbcConnection);
  }

  /**
   * Loads all table providers using service loader. This is the default configured in {@link
   * JdbcDriver#connect(String, Properties)}.
   */
  public static class LoadAllProviders extends InitialEmptySchema implements SchemaFactory {

    @Override
    public TableProvider toTableProvider(JdbcConnection connection) {
      MetaStore metaStore = new InMemoryMetaStore();
      for (TableProvider provider :
          ServiceLoader.load(TableProvider.class, getClass().getClassLoader())) {
        metaStore.registerProvider(provider);
      }
      return metaStore;
    }

    /** This is what Calcite calls to create an instance of the default top level schema. */
    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
      return this;
    }
  }

  /**
   * This is the override to create an empty schmea, used in {@link
   * JdbcDriver#connect(TableProvider)} to avoid loading all table providers.
   */
  public static class EmptySchema extends InitialEmptySchema implements SchemaFactory {

    private static final TableProvider READ_ONLY_TABLE_PROVIDER =
        new ReadOnlyTableProvider("empty", ImmutableMap.of());

    @Override
    public TableProvider toTableProvider(JdbcConnection connection) {
      return READ_ONLY_TABLE_PROVIDER;
    }

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
      return this;
    }
  }

  /**
   * Empty schema that {@link CalciteConnection} is initialized with by Calcite using the factories
   * above.
   *
   * <p>This is a temporary indirection, allows us to later replace this empy schema in the
   * connection with the actual one configured by us.
   */
  public abstract static class InitialEmptySchema implements Schema {

    public abstract TableProvider toTableProvider(JdbcConnection connection);

    @Override
    public Table getTable(String name) {
      return illegal();
    }

    @Override
    public Set<String> getTableNames() {
      return illegal();
    }

    @Override
    public RelProtoDataType getType(String name) {
      return illegal();
    }

    @Override
    public Set<String> getTypeNames() {
      return illegal();
    }

    @Override
    public Collection<Function> getFunctions(String name) {
      return illegal();
    }

    @Override
    public Set<String> getFunctionNames() {
      return illegal();
    }

    @Override
    public Schema getSubSchema(String name) {
      return illegal();
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return Collections.emptySet();
    }

    @Override
    public Expression getExpression(SchemaPlus parentSchema, String name) {
      return illegal();
    }

    @Override
    public boolean isMutable() {
      return illegal();
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
      return illegal();
    }

    @SuppressWarnings("TypeParameterUnusedInFormals")
    private static <T> T illegal() {
      throw new IllegalStateException("Beam JDBC connection has not been initialized");
    }
  }
}
