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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator;

import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for {@link BeamSqlPrimitive}.
 *
 */
public class BeamSqlPrimitiveTest extends BeamSqlFnExecutorTestBase {

  @Test
  public void testPrimitiveInt() {
    BeamSqlPrimitive<Integer> expInt = BeamSqlPrimitive.of(SqlTypeName.INTEGER, 100);
    Assert.assertEquals(expInt.getValue(), expInt.evaluate(row, null).getValue());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPrimitiveTypeUnMatch1() {
    BeamSqlPrimitive expInt = BeamSqlPrimitive.of(SqlTypeName.INTEGER, 100L);
    Assert.assertEquals(expInt.getValue(), expInt.evaluate(row, null).getValue());
  }
  @Test(expected = IllegalArgumentException.class)
  public void testPrimitiveTypeUnMatch2() {
    BeamSqlPrimitive expInt = BeamSqlPrimitive.of(SqlTypeName.DECIMAL, 100L);
    Assert.assertEquals(expInt.getValue(), expInt.evaluate(row, null).getValue());
  }
  @Test(expected = IllegalArgumentException.class)
  public void testPrimitiveTypeUnMatch3() {
    BeamSqlPrimitive expInt = BeamSqlPrimitive.of(SqlTypeName.FLOAT, 100L);
    Assert.assertEquals(expInt.getValue(), expInt.evaluate(row, null).getValue());
  }
  @Test(expected = IllegalArgumentException.class)
  public void testPrimitiveTypeUnMatch4() {
    BeamSqlPrimitive expInt = BeamSqlPrimitive.of(SqlTypeName.DOUBLE, 100L);
    Assert.assertEquals(expInt.getValue(), expInt.evaluate(row, null).getValue());
  }


}
