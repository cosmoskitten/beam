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
package org.apache.beam.sdk.extensions.sql.impl.udf;

import com.google.auto.service.AutoService;

/**
 * IS_INF(X)
 *
 * <p>Returns TRUE if the value is positive or negative infinity. Returns NULL for NULL inputs.
 * input: Float, Double
 *
 * <p>Output: Boolean
 */
@AutoService(BeamBuiltinFunctionClass.class)
public class IsInf implements BeamBuiltinFunctionClass {
  private static final String SQL_FUNCTION_NAME = "IS_INF";

  @UserDefinedFunctionAnnotation(
    funcName = SQL_FUNCTION_NAME,
    parameterArray = {Double.class},
    returnType = Boolean.class
  )
  public Boolean isInf(Double value) {
    return Double.isInfinite(value);
  }

  @UserDefinedFunctionAnnotation(
    funcName = SQL_FUNCTION_NAME,
    parameterArray = {Float.class},
    returnType = Boolean.class
  )
  public Boolean isInf(Float value) {
    return Float.isInfinite(value);
  }
}
