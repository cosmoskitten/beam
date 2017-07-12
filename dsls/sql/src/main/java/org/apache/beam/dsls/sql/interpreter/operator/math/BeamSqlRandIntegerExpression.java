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
import java.util.Random;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamSqlMathUnaryExpression} for 'RAND_INTEGER([seed, ] numeric)'
 * function.
 */
public class BeamSqlRandIntegerExpression extends BeamSqlExpression {
  public static final Random RAND = new Random();
  private int seed = Integer.MAX_VALUE;

  public BeamSqlRandIntegerExpression(List<BeamSqlExpression> subExps) {
    super(subExps, SqlTypeName.INTEGER);
  }

  @Override
  public boolean accept() {
    return true;
  }

  @Override
  public BeamSqlPrimitive evaluate(BeamSqlRow inputRecord) {
    if (operands.size() == 2) {
      int rowSeed = op(0).evaluate(inputRecord).getInteger();
      if (seed != rowSeed) {
        RAND.setSeed(rowSeed);
      }

      return BeamSqlPrimitive.of(SqlTypeName.INTEGER,
          RAND.nextInt(op(1).evaluate(inputRecord).getInteger()));
    } else {
      return BeamSqlPrimitive.of(SqlTypeName.INTEGER,
          RAND.nextInt(op(0).evaluate(inputRecord).getInteger()));
    }
  }
}
