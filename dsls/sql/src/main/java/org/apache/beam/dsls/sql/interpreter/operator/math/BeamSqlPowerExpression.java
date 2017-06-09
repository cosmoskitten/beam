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

package org.apache.beam.dsls.sql.interpreter.operator.math;

import java.util.List;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;


/**
 * {@code BeamSqlMathBinaryExpression} for 'POWER' function.
 */
public class BeamSqlPowerExpression extends BeamSqlMathBinaryExpression {

  public BeamSqlPowerExpression(List<BeamSqlExpression> operands) {
    super(operands);
  }

  @Override public BeamSqlPrimitive<? extends Number> calculate(BeamSqlPrimitive leftOp,
      BeamSqlPrimitive rightOp) {
    BeamSqlPrimitive result = null;
    if (SqlTypeName.INT_TYPES.contains(leftOp.getOutputType()) && SqlTypeName.INT_TYPES
        .contains(rightOp.getOutputType())) {

      result = BeamSqlPrimitive.of(SqlTypeName.BIGINT, SqlFunctions.toLong(SqlFunctions
          .power(SqlFunctions.toLong(leftOp.getValue()), SqlFunctions.toLong(rightOp.getValue()))));

    } else if (SqlTypeName.APPROX_TYPES.contains(leftOp.getOutputType()) || SqlTypeName.APPROX_TYPES
        .contains(rightOp.getOutputType())) {

      result = BeamSqlPrimitive.of(SqlTypeName.DOUBLE, SqlFunctions
          .power(SqlFunctions.toDouble(leftOp.getValue()),
              SqlFunctions.toDouble(rightOp.getValue())));

    } else if (SqlTypeName.BIGINT.equals(leftOp.getOutputType()) && SqlTypeName.DECIMAL
        .equals(rightOp.getOutputType())) {

      result = BeamSqlPrimitive.of(SqlTypeName.DOUBLE,
          SqlFunctions.power(leftOp.getLong(), SqlFunctions.toBigDecimal(rightOp.getValue())));

    } else if (SqlTypeName.DECIMAL.equals(leftOp.getOutputType()) || SqlTypeName.DECIMAL
        .equals(rightOp.getOutputType())) {

      result = BeamSqlPrimitive.of(SqlTypeName.DOUBLE, SqlFunctions
          .power(SqlFunctions.toBigDecimal(leftOp.getValue()),
              SqlFunctions.toBigDecimal(rightOp.getValue())));
    }
    return result;
  }

}
