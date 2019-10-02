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
package org.apache.beam.sdk.extensions.sql.meta;

import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexProgram;

/**
 * This default implementation of {@link BeamSqlTableFilter} interface. Assumes that predicate
 * push-down is not supported.
 */
public class DefaultTableFilter implements BeamSqlTableFilter {
  private final RexProgram program;
  private final RexNode filter;

  public DefaultTableFilter(RexProgram program, RexNode filter) {
    this.program = program;
    this.filter = filter;
  }

  /**
   * Since predicate push-down is assumed not to be supported by default - return an unchanged
   * filter to be preserved.
   *
   * @return Predicate {@code RexNode} which is not supported
   */
  @Override
  public RexNode getNotSupported() {
    return filter;
  }
}
