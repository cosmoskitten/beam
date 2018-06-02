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

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.values.Row;

/**
 * Implementations of {@link BeamSqlExpressionEnvironment} using arrays.
 *
 * <p>Use of arrays is efficient and safe, as Calcite generates variables densely packed and
 * referenced by index.
 */
public class BeamSqlExpressionEnvironments {

  public static BeamSqlExpressionEnvironment empty() {
    return new ArrayEnvironment(new ArrayList<>(), 0, new ArrayList<>(), 0);
  }

  public static BeamSqlExpressionEnvironment withEmptyLocalRefs(int localRefCount) {
    return new ArrayEnvironment(new ArrayList<>(), 0, new ArrayList<>(localRefCount), 0);
  }

  private static class ArrayEnvironment implements BeamSqlExpressionEnvironment {

    private final ArrayList<BeamSqlPrimitive<?>> localRefs;
    private final ArrayList<Row> correlVariables;

    private int nextMutableLocalRef;
    private int nextMutableCorrelVariable;

    public ArrayEnvironment(
        ArrayList<Row> correlVariables,
        int nextMutableCorrelVariable,
        ArrayList<BeamSqlPrimitive<?>> localRefs,
        int nextMutableLocalRef) {
      this.correlVariables = correlVariables;
      this.nextMutableCorrelVariable = nextMutableLocalRef;
      this.localRefs = localRefs;
      this.nextMutableLocalRef = nextMutableCorrelVariable;
    }

    @Override
    public void setLocalRef(int localRefIndex, BeamSqlPrimitive<?> value) {
      checkArgument(
          localRefIndex >= nextMutableLocalRef,
          "Cannot set local ref %s; next mutable ref is %s in %s",
          localRefIndex,
          nextMutableLocalRef,
          this);

      localRefs.add(localRefIndex, value);
      nextMutableLocalRef = localRefIndex + 1;
    }

    @Override
    public BeamSqlPrimitive<?> getLocalRef(int localRefIndex) {
      checkArgument(
          localRefIndex < nextMutableLocalRef,
          "Cannot get local ref %s; only refs less than %s have been set in %s",
          localRefIndex,
          nextMutableLocalRef,
          this);

      return localRefs.get(localRefIndex);
    }

    @Override
    public void setCorrelVariable(int correlVariableId, Row value) {
      checkArgument(
          correlVariableId >= nextMutableCorrelVariable,
          "Cannot set local ref %s; next mutable ref is %s in %s",
          correlVariableId,
          nextMutableCorrelVariable,
          this);

      correlVariables.add(correlVariableId, value);
      nextMutableCorrelVariable = correlVariableId + 1;
    }

    @Override
    public Row getCorrelVariable(int correlVariableId) {
      checkArgument(
          correlVariableId < nextMutableCorrelVariable,
          "Cannot get correlation variable %s; only refs less than %s have been set in %s",
          correlVariableId,
          this);

      return correlVariables.get(correlVariableId);
    }

    @Override
    public BeamSqlExpressionEnvironment copyWithEmptyLocalRefs(int localRefCount) {
      return new ArrayEnvironment(
          correlVariables,
          nextMutableCorrelVariable,
          Lists.newArrayListWithCapacity(localRefCount),
          0);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("correlVariables", correlVariables)
          .add("nextMutableCorrelVariable", nextMutableCorrelVariable)
          .add("localRefs", localRefs)
          .add("nextMutableLocalRef", nextMutableLocalRef)
          .toString();
    }
  }
}
