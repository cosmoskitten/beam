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

package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.arithmetic;

import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;

/** Base class for all arithmetic operators. */
public abstract class BeamSqlArithmeticExpression extends BeamSqlExpression {
  private static final List<Schema.TypeName> ORDERED_APPROX_TYPES = new ArrayList<>();

  static {
    ORDERED_APPROX_TYPES.add(Schema.TypeName.BYTE);
    ORDERED_APPROX_TYPES.add(Schema.TypeName.INT16);
    ORDERED_APPROX_TYPES.add(Schema.TypeName.INT32);
    ORDERED_APPROX_TYPES.add(Schema.TypeName.INT64);
    ORDERED_APPROX_TYPES.add(Schema.TypeName.FLOAT);
    ORDERED_APPROX_TYPES.add(Schema.TypeName.DOUBLE);
    ORDERED_APPROX_TYPES.add(Schema.TypeName.DECIMAL);
  }

  protected BeamSqlArithmeticExpression(List<BeamSqlExpression> operands) {
    super(
        operands,
        deduceOutputType(operands.get(0).getOutputType(), operands.get(1).getOutputType()));
  }

  protected BeamSqlArithmeticExpression(List<BeamSqlExpression> operands, Schema.FieldType outputType) {
    super(operands, outputType);
  }

  @Override
  public BeamSqlPrimitive<? extends Number> evaluate(
      Row inputRow, BoundedWindow window, ImmutableMap<Integer, Object> correlateEnv) {
    BigDecimal left =
        SqlFunctions.toBigDecimal((Object) opValueEvaluated(0, inputRow, window, correlateEnv));
    BigDecimal right =
        SqlFunctions.toBigDecimal((Object) opValueEvaluated(1, inputRow, window, correlateEnv));

    BigDecimal result = calc(left, right);
    return getCorrectlyTypedResult(result);
  }

  protected abstract BigDecimal calc(BigDecimal left, BigDecimal right);

  protected static Schema.FieldType deduceOutputType(Schema.FieldType left, Schema.FieldType right) {
    int leftIndex = ORDERED_APPROX_TYPES.indexOf(left.getTypeName());
    int rightIndex = ORDERED_APPROX_TYPES.indexOf(right.getTypeName());
    if ((left.getTypeName() == Schema.TypeName.FLOAT.FLOAT || right.getTypeName() == Schema.TypeName.FLOAT)
        && (left.getTypeName() == Schema.TypeName.DECIMAL || right.getTypeName() == Schema.TypeName.DECIMAL)) {
      return Schema.FieldType.DOUBLE;
    }

    if (leftIndex < rightIndex) {
      return right;
    } else if (leftIndex > rightIndex) {
      return left;
    } else {
      return left;
    }
  }

  @Override
  public boolean accept() {
    if (operands.size() != 2) {
      return false;
    }

    for (BeamSqlExpression operand : operands) {
      if (!SqlTypeName.NUMERIC_TYPES.contains(operand.getOutputType())) {
        return false;
      }
    }
    return true;
  }

  protected BeamSqlPrimitive<? extends Number> getCorrectlyTypedResult(BigDecimal rawResult) {
    Number actualValue;
    switch (outputType.getTypeName()) {
      case BYTE:
        actualValue = rawResult.byteValue();
        break;
      case INT16:
        actualValue = rawResult.shortValue();
        break;
      case INT32:
        actualValue = rawResult.intValue();
        break;
      case INT64:
        actualValue = rawResult.longValue();
        break;
      case FLOAT:
        actualValue = rawResult.floatValue();
        break;
      case DOUBLE:
        actualValue = rawResult.doubleValue();
        break;
      case DECIMAL:
      default:
        actualValue = rawResult;
    }
    return BeamSqlPrimitive.of(outputType, actualValue);
  }
}
