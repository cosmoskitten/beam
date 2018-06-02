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
package org.apache.beam.sdk.extensions.sql.impl.interpreter;

import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.values.Row;

/**
 * Environment in which a {@link BeamSqlExpression} is evaluated. This includes bindings of
 * correlation variables and local references.
 *
 * <p>For performance reasons, this class is mutable. However, it is an error to override any
 * binding of a local ref or correlation variable. Each may be set exactly once, in monotonically
 * increasing order.
 */
public interface BeamSqlExpressionEnvironment {
  /**
   * Sets the value for a local variable reference.
   *
   * <p>References must be set in monotonic order. Once a value is set for <i>n</i>, the values for
   * every reference less than or equal to <i>n</i> are permanently fixed.
   */
  void setLocalRef(int localRefIndex, BeamSqlPrimitive<?> value);

  /** Gets the value for a local variable reference. */
  BeamSqlPrimitive<?> getLocalRef(int localRefIndex);

  /**
   * Sets the value for a correlation variable.
   *
   * <p>Once set, it cannot be modified.
   */
  void setCorrelVariable(int correlVariableId, Row value);

  /** Gets the value for a correlation variable. */
  Row getCorrelVariable(int correlVariableId);

  /** An environment that shares correlation variables but local refs are cleared. */
  BeamSqlExpressionEnvironment copyWithEmptyLocalRefs(int localRefCount);
}
