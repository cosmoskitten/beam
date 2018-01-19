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

package org.apache.beam.sdk.extensions.sql.impl.transform.agg;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.utils.DecimalConverter;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Provides {@link VarianceFn} implementations for SQL types.
 */
public class Variance {

  private static final Map<SqlTypeName, VarianceFn> POPULATION_VARIANCE_MAP =
      ImmutableMap.<SqlTypeName, VarianceFn> builder()
          .put(SqlTypeName.INTEGER, VarianceFn.newPopulation(DecimalConverter.INTEGER))
          .put(SqlTypeName.SMALLINT, VarianceFn.newPopulation(DecimalConverter.SHORT))
          .put(SqlTypeName.TINYINT, VarianceFn.newPopulation(DecimalConverter.BYTE))
          .put(SqlTypeName.BIGINT, VarianceFn.newPopulation(DecimalConverter.LONG))
          .put(SqlTypeName.FLOAT, VarianceFn.newPopulation(DecimalConverter.FLOAT))
          .put(SqlTypeName.DOUBLE, VarianceFn.newPopulation(DecimalConverter.DOUBLE))
          .put(SqlTypeName.DECIMAL, VarianceFn.newPopulation(DecimalConverter.BIGDECIMAL))
      .build();

  private static final Map<SqlTypeName, VarianceFn> SAMPLE_VARIANCE_MAP =
      ImmutableMap.<SqlTypeName, VarianceFn> builder()
          .put(SqlTypeName.INTEGER, VarianceFn.newSample(DecimalConverter.INTEGER))
          .put(SqlTypeName.SMALLINT, VarianceFn.newSample(DecimalConverter.SHORT))
          .put(SqlTypeName.TINYINT, VarianceFn.newSample(DecimalConverter.BYTE))
          .put(SqlTypeName.BIGINT, VarianceFn.newSample(DecimalConverter.LONG))
          .put(SqlTypeName.FLOAT, VarianceFn.newSample(DecimalConverter.FLOAT))
          .put(SqlTypeName.DOUBLE, VarianceFn.newSample(DecimalConverter.DOUBLE))
          .put(SqlTypeName.DECIMAL, VarianceFn.newSample(DecimalConverter.BIGDECIMAL))
          .build();

  public static Combine.CombineFn populationVariance(SqlTypeName fieldType) {
    return getVarianceFn(fieldType, POPULATION_VARIANCE_MAP);
  }

  public static Combine.CombineFn sampleVariance(SqlTypeName fieldType) {
    return getVarianceFn(fieldType, SAMPLE_VARIANCE_MAP);
  }

  private static Combine.CombineFn getVarianceFn(
      SqlTypeName fieldType,
      Map<SqlTypeName, VarianceFn> varianceMap) {

    if (!varianceMap.containsKey(fieldType)) {
      throw new UnsupportedOperationException(
          "Variance calculation for " + fieldType + " is not supported");
    }

    return varianceMap.get(fieldType);
  }
}
