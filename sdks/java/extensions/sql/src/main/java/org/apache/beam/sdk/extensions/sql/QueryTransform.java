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


import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.sdk.extensions.sql.QueryValidationHelper.validateQuery;

import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A {@link PTransform} representing an execution plan for a SQL query.
 *
 * <p>The table names in the input {@code PCollectionTuple} are only valid during the current
 * query.
 */
public class QueryTransform<T extends PInput> extends PTransform<T, PCollection<Row>> {
  static final String PCOLLECTION_NAME = "PCOLLECTION";

  private BeamSqlEnv sqlEnv;
  private String queryString;

  private QueryTransform(BeamSqlEnv sqlEnv, String queryString) {
    this.sqlEnv = sqlEnv;
    this.queryString = queryString;
  }

  @Override
  public PCollection<Row> expand(T input) {
    checkArgument(input instanceof PCollection || input instanceof PCollectionTuple);
    PCollectionTuple inputTuple = toPCollectionTuple(input);
    registerPCollectionTuple(sqlEnv, inputTuple);

    try {
      return
          sqlEnv
              .getPlanner()
              .convertToBeamRel(queryString)
              .buildBeamPipeline(inputTuple, sqlEnv);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private PCollectionTuple toPCollectionTuple(PInput input) {
    if (input instanceof PCollectionTuple) {
      return (PCollectionTuple) input;
    }

    validateQuery(sqlEnv, queryString);
    return PCollectionTuple.of(new TupleTag<>(PCOLLECTION_NAME), (PCollection<Row>) input);
  }

  private void registerPCollectionTuple(
      BeamSqlEnv sqlEnv,
      PCollectionTuple pCollectionTuple) {

    for (TupleTag<?> tag : pCollectionTuple.getAll().keySet()) {
      registerPCollection(
          sqlEnv,
          tag.getId(),
          (PCollection<Row>) pCollectionTuple.get(tag));
    }
  }

  private void registerPCollection(
      BeamSqlEnv sqlEnv,
      String name,
      PCollection<Row> pCollection) {

    sqlEnv.registerTable(
        name,
        new BeamPCollectionTable(pCollection, ((RowCoder) pCollection.getCoder()).getRowType()));
  }

  /**
   * Creates a {@link Builder} with SQL {@code queryString}.
   */
  public static Builder withQueryString(String queryString) {
    return new Builder().withQueryString(queryString);
  }

  /**
   * Constructs the {@link QueryTransform}.
   */
  public static class Builder {
    private BeamSqlEnv sqlEnv = new BeamSqlEnv();
    private String queryString;

    private Builder() {
    }

    Builder withQueryString(String queryString) {
      this.queryString = queryString;
      return this;
    }

    /**
     * register a UDF function used in this query.
     *
     * <p>Refer to {@link BeamSqlUdf} for more about how to implement a UDF in BeamSql.
     */
    public Builder registerUdf(String functionName, Class<? extends BeamSqlUdf> clazz) {
      sqlEnv.registerUdf(functionName, clazz);
      return this;
    }

    /**
     * Register {@link SerializableFunction} as a UDF function used in this query.
     * Note, {@link SerializableFunction} must have a constructor without arguments.
     */
    public Builder registerUdf(String functionName, SerializableFunction sfn) {
      sqlEnv.registerUdf(functionName, sfn);
      return this;
    }

    /**
     * register a {@link Combine.CombineFn} as UDAF function used in this query.
     */
    public Builder registerUdaf(String functionName, Combine.CombineFn combineFn) {
      sqlEnv.registerUdaf(functionName, combineFn);
      return this;
    }

    /**
     * Create the {@link PTransform} representing current SQL query.
     */
    public <T extends PInput> PTransform<T, PCollection<Row>> toPTransform() {
      return new QueryTransform<>(sqlEnv, this.queryString);
    }
  }
}
