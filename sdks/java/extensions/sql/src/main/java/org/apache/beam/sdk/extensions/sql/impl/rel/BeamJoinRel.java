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
package org.apache.beam.sdk.extensions.sql.impl.rel;

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.sql.BeamSqlSeekableTable;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamJoinTransforms;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.base.Optional;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.volcano.RelSubset;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.CorrelationId;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Join;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexFieldAccess;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.Pair;

/**
 * An abstract {@code BeamRelNode} to implement Join Rels.
 *
 * <p>Support for join can be categorized into 4 cases:
 *
 * <ul>
 *   <li>BoundedTable JOIN BoundedTable
 *   <li>UnboundedTable JOIN UnboundedTable
 *   <li>BoundedTable JOIN UnboundedTable
 *   <li>SeekableTable JOIN non SeekableTable
 * </ul>
 */
public abstract class BeamJoinRel extends Join implements BeamRelNode {

  protected BeamJoinRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traits, left, right, condition, variablesSet, joinType);
  }

  @Override
  public List<RelNode> getPCollectionInputs() {
    if (isSideInputLookupJoin()) {
      return ImmutableList.of(
          BeamSqlRelUtils.getBeamRelInput(getInputs().get(nonSeekableInputIndex().get())));
    } else {
      return BeamRelNode.super.getPCollectionInputs();
    }
  }

  protected boolean isSideInputLookupJoin() {
    return seekableInputIndex().isPresent() && nonSeekableInputIndex().isPresent();
  }

  protected Optional<Integer> seekableInputIndex() {
    BeamRelNode leftRelNode = BeamSqlRelUtils.getBeamRelInput(left);
    BeamRelNode rightRelNode = BeamSqlRelUtils.getBeamRelInput(right);
    return seekable(leftRelNode)
        ? Optional.of(0)
        : seekable(rightRelNode) ? Optional.of(1) : Optional.absent();
  }

  protected Optional<Integer> nonSeekableInputIndex() {
    BeamRelNode leftRelNode = BeamSqlRelUtils.getBeamRelInput(left);
    BeamRelNode rightRelNode = BeamSqlRelUtils.getBeamRelInput(right);
    return !seekable(leftRelNode)
        ? Optional.of(0)
        : !seekable(rightRelNode) ? Optional.of(1) : Optional.absent();
  }

  /** check if {@code BeamRelNode} implements {@code BeamSeekableTable}. */
  public static boolean seekable(BeamRelNode relNode) {
    if (relNode instanceof BeamIOSourceRel) {
      BeamIOSourceRel srcRel = (BeamIOSourceRel) relNode;
      BeamSqlTable sourceTable = srcRel.getBeamSqlTable();
      if (sourceTable instanceof BeamSqlSeekableTable) {
        return true;
      }
    }
    return false;
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    NodeStats leftEstimates = BeamSqlRelUtils.getNodeStats(this.left, mq);
    NodeStats rightEstimates = BeamSqlRelUtils.getNodeStats(this.right, mq);
    NodeStats selfEstimates = BeamSqlRelUtils.getNodeStats(this, mq);
    NodeStats summation = selfEstimates.plus(leftEstimates).plus(rightEstimates);
    return BeamCostModel.FACTORY.makeCost(summation.getRowCount(), summation.getRate());
  }

  @Override
  public NodeStats estimateNodeStats(RelMetadataQuery mq) {
    double selectivity = mq.getSelectivity(this, getCondition());
    NodeStats leftEstimates = BeamSqlRelUtils.getNodeStats(this.left, mq);
    NodeStats rightEstimates = BeamSqlRelUtils.getNodeStats(this.right, mq);

    if (leftEstimates.isUnknown() || rightEstimates.isUnknown()) {
      return NodeStats.UNKNOWN;
    }
    // If any of the inputs are unbounded row count becomes zero (one of them would be zero)
    // If one is bounded and one unbounded the rate will be window of the bounded (= its row count)
    // multiplied by the rate of the unbounded one
    // If both are unbounded, the rate will be multiplication of each rate into the window of the
    // other.
    return NodeStats.create(
        leftEstimates.getRowCount() * rightEstimates.getRowCount() * selectivity,
        (leftEstimates.getRate() * rightEstimates.getWindow()
                + rightEstimates.getRate() * leftEstimates.getWindow())
            * selectivity,
        leftEstimates.getWindow() * rightEstimates.getWindow() * selectivity);
  }

  /**
   * This method checks if a join is legal and can be converted into Beam SQL. It is used during
   * planning and applying {@link
   * org.apache.beam.sdk.extensions.sql.impl.rule.BeamJoinAssociateRule} and {@link
   * org.apache.beam.sdk.extensions.sql.impl.rule.BeamJoinPushThroughJoinRule}
   */
  public static boolean isJoinLegal(Join join) {
    try {
      extractJoinRexNodes(join.getCondition());
    } catch (UnsupportedOperationException e) {
      return false;
    }
    return true;
  }

  protected class ExtractJoinKeys
      extends PTransform<PCollectionList<Row>, PCollectionList<KV<Row, Row>>> {

    @Override
    public PCollectionList<KV<Row, Row>> expand(PCollectionList<Row> pinput) {
      BeamRelNode leftRelNode = BeamSqlRelUtils.getBeamRelInput(left);

      Schema leftSchema = CalciteUtils.toSchema(left.getRowType());
      Schema rightSchema = CalciteUtils.toSchema(right.getRowType());

      assert pinput.size() == 2;
      PCollection<Row> leftRows = pinput.get(0);
      PCollection<Row> rightRows = pinput.get(1);

      int leftRowColumnCount = leftRelNode.getRowType().getFieldCount();

      // extract the join fields
      List<Pair<RexNode, RexNode>> pairs = extractJoinRexNodes(condition);

      // build the extract key type
      // the name of the join field is not important
      Schema extractKeySchemaLeft =
          pairs.stream()
              .map(pair -> getFieldBasedOnRexNode(leftSchema, pair.getKey(), 0))
              .collect(toSchema());
      Schema extractKeySchemaRight =
          pairs.stream()
              .map(pair -> getFieldBasedOnRexNode(rightSchema, pair.getValue(), leftRowColumnCount))
              .collect(toSchema());

      SchemaCoder<Row> extractKeyRowCoder = SchemaCoder.of(extractKeySchemaLeft);

      // BeamSqlRow -> KV<BeamSqlRow, BeamSqlRow>
      PCollection<KV<Row, Row>> extractedLeftRows =
          leftRows
              .apply(
                  "left_TimestampCombiner",
                  Window.<Row>configure().withTimestampCombiner(TimestampCombiner.EARLIEST))
              .apply(
                  "left_ExtractJoinFields",
                  MapElements.via(
                      new BeamJoinTransforms.ExtractJoinFields(
                          true, pairs, extractKeySchemaLeft, 0)))
              .setCoder(KvCoder.of(extractKeyRowCoder, leftRows.getCoder()));

      PCollection<KV<Row, Row>> extractedRightRows =
          rightRows
              .apply(
                  "right_TimestampCombiner",
                  Window.<Row>configure().withTimestampCombiner(TimestampCombiner.EARLIEST))
              .apply(
                  "right_ExtractJoinFields",
                  MapElements.via(
                      new BeamJoinTransforms.ExtractJoinFields(
                          false, pairs, extractKeySchemaRight, leftRowColumnCount)))
              .setCoder(KvCoder.of(extractKeyRowCoder, rightRows.getCoder()));

      return PCollectionList.of(extractedLeftRows).and(extractedRightRows);
    }
  }

  protected Schema buildNullSchema(Schema schema) {
    Schema.Builder builder = Schema.builder();

    builder.addFields(
        schema.getFields().stream().map(f -> f.withNullable(true)).collect(Collectors.toList()));

    return builder.build();
  }

  protected static <K, V> PCollection<KV<K, V>> setValueCoder(
      PCollection<KV<K, V>> kvs, Coder<V> valueCoder) {
    // safe case because PCollection of KV always has KvCoder
    KvCoder<K, V> coder = (KvCoder<K, V>) kvs.getCoder();

    return kvs.setCoder(KvCoder.of(coder.getKeyCoder(), valueCoder));
  }

  private static Field getFieldBasedOnRexNode(
      Schema schema, RexNode rexNode, int leftRowColumnCount) {
    if (rexNode instanceof RexInputRef) {
      return schema.getField(((RexInputRef) rexNode).getIndex() - leftRowColumnCount);
    } else if (rexNode instanceof RexFieldAccess) {
      // need to extract field of Struct/Row.
      return getFieldBasedOnRexFieldAccess(schema, (RexFieldAccess) rexNode, leftRowColumnCount);
    }

    throw new UnsupportedOperationException("Does not support " + rexNode.getType() + " in JOIN.");
  }

  private static Field getFieldBasedOnRexFieldAccess(
      Schema schema, RexFieldAccess rexFieldAccess, int leftRowColumnCount) {
    ArrayDeque<RexFieldAccess> fieldAccessStack = new ArrayDeque<>();
    fieldAccessStack.push(rexFieldAccess);

    RexFieldAccess curr = rexFieldAccess;
    while (curr.getReferenceExpr() instanceof RexFieldAccess) {
      curr = (RexFieldAccess) curr.getReferenceExpr();
      fieldAccessStack.push(curr);
    }

    // curr.getReferenceExpr() is not a RexFieldAccess. Check if it is RexInputRef, which is only
    // allowed RexNode type in RexFieldAccess.
    if (!(curr.getReferenceExpr() instanceof RexInputRef)) {
      throw new UnsupportedOperationException(
          "Does not support " + curr.getReferenceExpr().getType() + " in JOIN.");
    }

    // curr.getReferenceExpr() is a RexInputRef.
    RexInputRef inputRef = (RexInputRef) curr.getReferenceExpr();
    Field curField = schema.getField(inputRef.getIndex() - leftRowColumnCount);

    // pop RexFieldAccess from stack one by one to know the final field type.
    while (fieldAccessStack.size() > 0) {
      curr = fieldAccessStack.pop();
      curField = curField.getType().getRowSchema().getField(curr.getField().getIndex());
    }

    return curField;
  }

  static List<Pair<RexNode, RexNode>> extractJoinRexNodes(RexNode condition) {
    // it's a CROSS JOIN because: condition == true
    if (condition instanceof RexLiteral && (Boolean) ((RexLiteral) condition).getValue()) {
      throw new UnsupportedOperationException("CROSS JOIN is not supported!");
    }

    RexCall call = (RexCall) condition;
    List<Pair<RexNode, RexNode>> pairs = new ArrayList<>();
    if ("AND".equals(call.getOperator().getName())) {
      List<RexNode> operands = call.getOperands();
      for (RexNode rexNode : operands) {
        Pair<RexNode, RexNode> pair = extractJoinPairOfRexNodes((RexCall) rexNode);
        pairs.add(pair);
      }
    } else if ("=".equals(call.getOperator().getName())) {
      pairs.add(extractJoinPairOfRexNodes(call));
    } else {
      throw new UnsupportedOperationException(
          "Operator " + call.getOperator().getName() + " is not supported in join condition");
    }

    return pairs;
  }

  private static Pair<RexNode, RexNode> extractJoinPairOfRexNodes(RexCall rexCall) {
    if (!rexCall.getOperator().getName().equals("=")) {
      throw new UnsupportedOperationException("Non equi-join is not supported");
    }

    if (isIllegalJoinConjunctionClause(rexCall)) {
      throw new UnsupportedOperationException(
          "Only support column reference or struct field access in conjunction clause");
    }

    int leftIndex = getColumnIndex(rexCall.getOperands().get(0));
    int rightIndex = getColumnIndex(rexCall.getOperands().get(1));
    if (leftIndex < rightIndex) {
      return new Pair<>(rexCall.getOperands().get(0), rexCall.getOperands().get(1));
    } else {
      return new Pair<>(rexCall.getOperands().get(1), rexCall.getOperands().get(0));
    }
  }

  // Only support {RexInputRef | RexFieldAccess} = {RexInputRef | RexFieldAccess}
  private static boolean isIllegalJoinConjunctionClause(RexCall rexCall) {
    return (!(rexCall.getOperands().get(0) instanceof RexInputRef)
            && !(rexCall.getOperands().get(0) instanceof RexFieldAccess))
        || (!(rexCall.getOperands().get(1) instanceof RexInputRef)
            && !(rexCall.getOperands().get(1) instanceof RexFieldAccess));
  }

  private static int getColumnIndex(RexNode rexNode) {
    if (rexNode instanceof RexInputRef) {
      return ((RexInputRef) rexNode).getIndex();
    } else if (rexNode instanceof RexFieldAccess) {
      return getColumnIndex(((RexFieldAccess) rexNode).getReferenceExpr());
    }

    throw new UnsupportedOperationException("Cannot get column index from " + rexNode.getType());
  }

  /**
   * This method returns the Boundedness of a RelNode. It is used during planning and applying
   * {@link org.apache.beam.sdk.extensions.sql.impl.rule.BeamCoGBKJoinRule} and {@link
   * org.apache.beam.sdk.extensions.sql.impl.rule.BeamSideInputJoinRule}
   *
   * <p>The Volcano planner works in a top-down fashion. It starts by transforming the root and move
   * towards the leafs of the plan. Due to this when transforming a logical join its inputs are
   * still in the logical convention. So, Recursively visit the inputs of the RelNode till
   * BeamIOSourceRel is encountered and propagate the boundedness upwards.
   *
   * <p>The Boundedness of each child of a RelNode is stored in a list. If any of the children are
   * Unbounded, the RelNode is Unbounded. Else, the RelNode is Bounded.
   *
   * @param relNode the RelNode whose Boundedness has to be determined
   * @return {@code PCollection.isBounded}
   */
  public static PCollection.IsBounded getBoundednessOfRelNode(RelNode relNode) {
    if (relNode instanceof BeamRelNode) {
      return (((BeamRelNode) relNode).isBounded());
    }
    List<PCollection.IsBounded> boundednessOfInputs = new ArrayList<>();
    for (RelNode inputRel : relNode.getInputs()) {
      if (inputRel instanceof RelSubset) {
        // Consider the RelNode with best cost in the RelSubset. If best cost RelNode cannot be
        // determined, consider the first RelNode in the RelSubset
        RelNode rel = ((RelSubset) inputRel).getBest();
        if (rel == null) {
          rel = ((RelSubset) inputRel).getRelList().get(0);
        }
        boundednessOfInputs.add(getBoundednessOfRelNode(rel));
      } else {
        boundednessOfInputs.add(getBoundednessOfRelNode(inputRel));
      }
    }
    // If one of the input is Unbounded, the result is Unbounded.
    return (boundednessOfInputs.contains(PCollection.IsBounded.UNBOUNDED)
        ? PCollection.IsBounded.UNBOUNDED
        : PCollection.IsBounded.BOUNDED);
  }

  /**
   * This method returns whether any of the children of the relNode are Seekable. It is used during
   * planning and applying {@link org.apache.beam.sdk.extensions.sql.impl.rule.BeamCoGBKJoinRule}
   * and {@link org.apache.beam.sdk.extensions.sql.impl.rule.BeamSideInputJoinRule} and {@link
   * org.apache.beam.sdk.extensions.sql.impl.rule.BeamSideInputLookupJoinRule}
   *
   * @param relNode the relNode whose children can be Seekable
   * @return A boolean
   */
  public static boolean containsSeekableInput(RelNode relNode) {
    for (RelNode relInput : relNode.getInputs()) {
      if (relInput instanceof RelSubset) {
        relInput = ((RelSubset) relInput).getBest();
      }
      // input is Seekable
      if (relInput != null
          && relInput instanceof BeamRelNode
          && (BeamJoinRel.seekable((BeamRelNode) relInput))) {
        return true;
      }
    }
    // None of the inputs are Seekable
    return false;
  }
}
