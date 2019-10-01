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
package org.apache.beam.sdk.extensions.sql.impl.rule;

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamAggregationRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Aggregate;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.RelFactories;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.TableScan;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RelBuilderFactory;

/**
 * Aggregation rule that doesn't include projection.
 *
 * <p>Doesn't support windowing, as we extract window information from projection node.
 *
 * <p>{@link BeamAggregationRule} supports projection and windowing.
 */
public class BeamBasicAggregationRule extends RelOptRule {
  public static final BeamBasicAggregationRule INSTANCE =
      new BeamBasicAggregationRule(Aggregate.class, RelFactories.LOGICAL_BUILDER);

  public BeamBasicAggregationRule(
      Class<? extends Aggregate> aggregateClass, RelBuilderFactory relBuilderFactory) {
    super(operand(aggregateClass, operand(TableScan.class, any())), relBuilderFactory, null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate aggregate = call.rel(0);
    TableScan tableScan = call.rel(1);

    RelNode newTableScan = tableScan.copy(tableScan.getTraitSet(), tableScan.getInputs());

    call.transformTo(
        new BeamAggregationRel(
            aggregate.getCluster(),
            aggregate.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
            convert(
                newTableScan, newTableScan.getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
            aggregate.indicator,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList(),
            null,
            -1));
  }
}
