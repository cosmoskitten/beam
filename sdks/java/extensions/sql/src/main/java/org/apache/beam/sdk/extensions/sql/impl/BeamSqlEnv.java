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

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.udf.BeamBuiltinFunctionProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.UdfUdafProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExecutableStatement;

/**
 * Contains the metadata of tables/UDF functions, and exposes APIs to
 * query/validate/optimize/translate SQL statements.
 */
@Internal
@Experimental
public class BeamSqlEnv {
  final JdbcConnection connection;
  final QueryPlanner planner;

  private BeamSqlEnv(JdbcConnection connection, QueryPlanner planner) {
    this.connection = connection;
    this.planner = planner;
  }

  public static BeamSqlEnv readOnly(String tableType, Map<String, BeamSqlTable> tables) {
    return withTableProvider(new ReadOnlyTableProvider(tableType, tables));
  }

  public static BeamSqlEnv withTableProvider(TableProvider tableProvider) {
    BeamSqlEnvBuilder beamSqlEnvBuilder = new BeamSqlEnvBuilder(tableProvider);
    return beamSqlEnvBuilder.build();
  }

  public static BeamSqlEnv inMemory(TableProvider... tableProviders) {
    InMemoryMetaStore inMemoryMetaStore = new InMemoryMetaStore();
    for (TableProvider tableProvider : tableProviders) {
      inMemoryMetaStore.registerProvider(tableProvider);
    }

    return withTableProvider(inMemoryMetaStore);
  }

  public BeamRelNode parseQuery(String query) throws ParseException {
    try {
      return planner.convertToBeamRel(query);
    } catch (Exception e) {
      throw new ParseException(String.format("Unable to parse query %s", query), e);
    }
  }

  public boolean isDdl(String sqlStatement) throws ParseException {
    try {
      return planner.parse(sqlStatement) instanceof SqlExecutableStatement;
    } catch (Exception e) {
      throw new ParseException("Unable to parse statement", e);
    }
  }

  public void executeDdl(String sqlStatement) throws ParseException {
    try {
      SqlExecutableStatement ddl = (SqlExecutableStatement) planner.parse(sqlStatement);
      ddl.execute(getContext());
    } catch (Exception e) {
      throw new ParseException("Unable to parse DDL statement", e);
    }
  }

  public CalcitePrepare.Context getContext() {
    return connection.createPrepareContext();
  }

  public Map<String, String> getPipelineOptions() {
    return connection.getPipelineOptionsMap();
  }

  public String explain(String sqlString) throws ParseException {
    try {
      return RelOptUtil.toString(planner.convertToBeamRel(sqlString));
    } catch (Exception e) {
      throw new ParseException("Unable to parse statement", e);
    }
  }

  /** BeamSqlEnv's Builder. */
  public static class BeamSqlEnvBuilder {
    private final JdbcConnection jdbcConnection;
    private String plannerName;

    public static BeamSqlEnvBuilder builder(TableProvider tableProvider) {
      return new BeamSqlEnvBuilder(tableProvider);
    }

    private BeamSqlEnvBuilder(TableProvider tableProvider) {
      jdbcConnection = JdbcDriver.connect(tableProvider);
    }

    public BeamSqlEnvBuilder registerBuiltinUdf(Map<String, List<Method>> methods) {
      for (Map.Entry<String, List<Method>> entry : methods.entrySet()) {
        for (Method method : entry.getValue()) {
          jdbcConnection.getCurrentSchemaPlus().add(entry.getKey(), UdfImpl.create(method));
        }
      }

      return this;
    }

    public BeamSqlEnvBuilder addSchema(String name, TableProvider tableProvider) {
      jdbcConnection.setSchema(name, tableProvider);

      return this;
    }

    public BeamSqlEnvBuilder setCurrentSchema(String name) {
      try {
        jdbcConnection.setSchema(name);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }

      return this;
    }

    /** Register a UDF function which can be used in SQL expression. */
    public BeamSqlEnvBuilder registerUdf(String functionName, Class<?> clazz, String method) {
      jdbcConnection.getCurrentSchemaPlus().add(functionName, UdfImpl.create(clazz, method));

      return this;
    }

    /** Register a UDF function which can be used in SQL expression. */
    public BeamSqlEnvBuilder registerUdf(String functionName, Class<? extends BeamSqlUdf> clazz) {
      return registerUdf(functionName, clazz, BeamSqlUdf.UDF_METHOD);
    }

    public BeamSqlEnvBuilder registerUdf(String functionName, SerializableFunction sfn) {
      return registerUdf(functionName, sfn.getClass(), "apply");
    }

    /**
     * Register a UDAF function which can be used in GROUP-BY expression. See {@link
     * org.apache.beam.sdk.transforms.Combine.CombineFn} on how to implement a UDAF.
     */
    public BeamSqlEnvBuilder registerUdaf(String functionName, Combine.CombineFn combineFn) {
      jdbcConnection.getCurrentSchemaPlus().add(functionName, new UdafImpl(combineFn));

      return this;
    }

    /** Load all UDF/UDAF from {@link UdfUdafProvider}. */
    public BeamSqlEnvBuilder loadUdfUdafFromProvider() {
      ServiceLoader.<UdfUdafProvider>load(UdfUdafProvider.class)
          .forEach(
              ins -> {
                ins.getBeamSqlUdfs().forEach((udfName, udfClass) -> registerUdf(udfName, udfClass));
                ins.getSerializableFunctionUdfs()
                    .forEach((udfName, udfFn) -> registerUdf(udfName, udfFn));
                ins.getUdafs().forEach((udafName, udafFn) -> registerUdaf(udafName, udafFn));
              });

      return this;
    }

    public BeamSqlEnvBuilder loadBeamBuiltinFunctions() {
      for (BeamBuiltinFunctionProvider provider :
          ServiceLoader.load(BeamBuiltinFunctionProvider.class)) {
        registerBuiltinUdf(provider.getBuiltinMethods());
      }

      return this;
    }

    public BeamSqlEnvBuilder setPlannerName(String name) {
      plannerName = name;
      return this;
    }

    public BeamSqlEnv build() {
      if (plannerName.equals("CalcitePlanner")) {
        return new BeamSqlEnv(jdbcConnection, new CalciteQueryPlanner(jdbcConnection));
      } else {
        throw new UnsupportedOperationException("Does not support planner impl: " + plannerName);
      }
    }
  }
}
