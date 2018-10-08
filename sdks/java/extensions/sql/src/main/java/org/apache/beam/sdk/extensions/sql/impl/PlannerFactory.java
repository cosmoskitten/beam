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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;

/** Factory that creates an instance of {@link Planner} */
class PlannerFactory {
  public static Planner create(FrameworkConfig config, BeamSqlPipelineOptions options) {
    Constructor constructor = getPlannerConstructorFromPipelineOptions(options);
    return constructPlanner(config, constructor);
  }

  private static Constructor getPlannerConstructorFromPipelineOptions(
      BeamSqlPipelineOptions options) {
    String plannerImplClassName = null;
    try {
      plannerImplClassName = options.getPlannerImplClassName();
      return Class.forName(plannerImplClassName).getConstructor(FrameworkConfig.class);
    } catch (NoSuchMethodException | ClassNotFoundException e) {
      throw new RuntimeException(
          "Cannot initialize plannerImplConstructor by " + plannerImplClassName, e);
    }
  }

  private static Planner constructPlanner(FrameworkConfig config, Constructor plannerConstructor) {
    try {
      return (Planner) plannerConstructor.newInstance(config);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalArgumentException(
          "Using an illegal plannerImplConstructor: " + plannerConstructor.toString(), e);
    }
  }
}
