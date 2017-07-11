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

package org.apache.beam.dsls.sql.interpreter.operator.arithmetic;

import java.util.List;

import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;

/**
 * '/' operator.
 */
public class BeamSqlDivideExpression extends BeamSqlArithmeticExpression {
  public BeamSqlDivideExpression(List<BeamSqlExpression> operands) {
    super(operands, operands.get(0).getOutputType());
  }

  @Override public Long calc(Long left, Long right) {
    return left / right;
  }

  @Override public Double calc(Number left, Number right) {
    if (right.doubleValue() == 0) {
      throw new IllegalArgumentException("divisor cannot be 0");
    }
    return left.doubleValue() / right.doubleValue();
  }
}
