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

package org.apache.beam.dsls.sql.rel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.dsls.sql.BeamSqlEnv;
import org.apache.beam.dsls.sql.transform.BeamJoinTransforms;
import org.apache.beam.dsls.sql.utils.CalciteUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.sd.BeamRow;
import org.apache.beam.sdk.sd.BeamRowCoder;
import org.apache.beam.sdk.sd.BeamRowType;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.IncompatibleWindowException;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

/**
 * {@code BeamRelNode} to replace a {@code Join} node.
 *
 * <p>Support for join can be categorized into 3 cases:
 * <ul>
 *   <li>BoundedTable JOIN BoundedTable</li>
 *   <li>UnboundedTable JOIN UnboundedTable</li>
 *   <li>BoundedTable JOIN UnboundedTable</li>
 * </ul>
 *
 * <p>For the first two cases, a standard join is utilized as long as the windowFn of the both
 * sides match.
 *
 * <p>For the third case, {@code sideInput} is utilized to implement the join, so there are some
 * constraints:
 *
 * <ul>
 *   <li>{@code FULL OUTER JOIN} is not supported.</li>
 *   <li>If it's a {@code LEFT OUTER JOIN}, the unbounded table should on the left side.</li>
 *   <li>If it's a {@code RIGHT OUTER JOIN}, the unbounded table should on the right side.</li>
 * </ul>
 *
 *
 * <p>There are also some general constraints:
 *
 * <ul>
 *  <li>Only equi-join is supported.</li>
 *  <li>CROSS JOIN is not supported.</li>
 * </ul>
 */
public class BeamJoinRel extends Join implements BeamRelNode {
  public BeamJoinRel(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right,
      RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
    super(cluster, traits, left, right, condition, variablesSet, joinType);
  }

  @Override public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
      RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return new BeamJoinRel(getCluster(), traitSet, left, right, conditionExpr, variablesSet,
        joinType);
  }

  @Override public PCollection<BeamRow> buildBeamPipeline(PCollectionTuple inputPCollections,
      BeamSqlEnv sqlEnv)
      throws Exception {
    BeamRelNode leftRelNode = BeamSqlRelUtils.getBeamRelInput(left);
    BeamRowType leftRowType = CalciteUtils.toBeamRowType(left.getRowType());
    PCollection<BeamRow> leftRows = leftRelNode.buildBeamPipeline(inputPCollections, sqlEnv);

    final BeamRelNode rightRelNode = BeamSqlRelUtils.getBeamRelInput(right);
    PCollection<BeamRow> rightRows = rightRelNode.buildBeamPipeline(inputPCollections, sqlEnv);

    String stageName = BeamSqlRelUtils.getStageName(this);
    WindowFn leftWinFn = leftRows.getWindowingStrategy().getWindowFn();
    WindowFn rightWinFn = rightRows.getWindowingStrategy().getWindowFn();

    // extract the join fields
    List<Pair<Integer, Integer>> pairs = extractJoinColumns(
        leftRelNode.getRowType().getFieldCount());

    // build the extract key type
    // the name of the join field is not important
    List<String> names = new ArrayList<>(pairs.size());
    List<Integer> types = new ArrayList<>(pairs.size());
    for (int i = 0; i < pairs.size(); i++) {
      names.add("c" + i);
      types.add(leftRowType.getFieldsType().get(pairs.get(i).getKey()));
    }
    BeamRowType extractKeyRowType = BeamRowType.create(names, types);

    Coder extractKeyRowCoder = new BeamRowCoder(extractKeyRowType);

    // BeamSqlRow -> KV<BeamSqlRow, BeamSqlRow>
    PCollection<KV<BeamRow, BeamRow>> extractedLeftRows = leftRows
        .apply(stageName + "_left_ExtractJoinFields",
            MapElements.via(new BeamJoinTransforms.ExtractJoinFields(true, pairs)))
        .setCoder(KvCoder.of(extractKeyRowCoder, leftRows.getCoder()));

    PCollection<KV<BeamRow, BeamRow>> extractedRightRows = rightRows
        .apply(stageName + "_right_ExtractJoinFields",
            MapElements.via(new BeamJoinTransforms.ExtractJoinFields(false, pairs)))
        .setCoder(KvCoder.of(extractKeyRowCoder, rightRows.getCoder()));

    // prepare the NullRows
    BeamRow leftNullRow = buildNullRow(leftRelNode);
    BeamRow rightNullRow = buildNullRow(rightRelNode);

    // a regular join
    if ((leftRows.isBounded() == PCollection.IsBounded.BOUNDED
            && rightRows.isBounded() == PCollection.IsBounded.BOUNDED)
           || (leftRows.isBounded() == PCollection.IsBounded.UNBOUNDED
                && rightRows.isBounded() == PCollection.IsBounded.UNBOUNDED)) {
      try {
        leftWinFn.verifyCompatibility(rightWinFn);
      } catch (IncompatibleWindowException e) {
        throw new IllegalArgumentException(
            "WindowFns must match for a bounded-vs-bounded/unbounded-vs-unbounded join.", e);
      }

      return standardJoin(extractedLeftRows, extractedRightRows,
          leftNullRow, rightNullRow, stageName);
    } else if (
        (leftRows.isBounded() == PCollection.IsBounded.BOUNDED
        && rightRows.isBounded() == PCollection.IsBounded.UNBOUNDED)
        || (leftRows.isBounded() == PCollection.IsBounded.UNBOUNDED
            && rightRows.isBounded() == PCollection.IsBounded.BOUNDED)
        ) {
      // if one of the sides is Bounded & the other is Unbounded
      // then do a sideInput join
      // when doing a sideInput join, the windowFn does not need to match
      // Only support INNER JOIN & LEFT OUTER JOIN where left side of the join must be
      // the unbounded
      if (joinType == JoinRelType.FULL) {
        throw new UnsupportedOperationException("FULL OUTER JOIN is not supported when join "
            + "a bounded table with an unbounded table.");
      }

      if ((joinType == JoinRelType.LEFT
          && leftRows.isBounded() == PCollection.IsBounded.BOUNDED)
          || (joinType == JoinRelType.RIGHT
          && rightRows.isBounded() == PCollection.IsBounded.BOUNDED)) {
        throw new UnsupportedOperationException(
            "LEFT side of an OUTER JOIN must be Unbounded table.");
      }

      return sideInputJoin(extractedLeftRows, extractedRightRows,
          leftNullRow, rightNullRow);
    } else {
      throw new UnsupportedOperationException(
          "The inputs to the JOIN have un-joinnable windowFns: " + leftWinFn + ", " + rightWinFn);
    }
  }

  private PCollection<BeamRow> standardJoin(
      PCollection<KV<BeamRow, BeamRow>> extractedLeftRows,
      PCollection<KV<BeamRow, BeamRow>> extractedRightRows,
      BeamRow leftNullRow, BeamRow rightNullRow, String stageName) {
    PCollection<KV<BeamRow, KV<BeamRow, BeamRow>>> joinedRows = null;
    switch (joinType) {
      case LEFT:
        joinedRows = org.apache.beam.sdk.extensions.joinlibrary.Join
            .leftOuterJoin(extractedLeftRows, extractedRightRows, rightNullRow);
        break;
      case RIGHT:
        joinedRows = org.apache.beam.sdk.extensions.joinlibrary.Join
            .rightOuterJoin(extractedLeftRows, extractedRightRows, leftNullRow);
        break;
      case FULL:
        joinedRows = org.apache.beam.sdk.extensions.joinlibrary.Join
            .fullOuterJoin(extractedLeftRows, extractedRightRows, leftNullRow,
            rightNullRow);
        break;
      case INNER:
      default:
        joinedRows = org.apache.beam.sdk.extensions.joinlibrary.Join
            .innerJoin(extractedLeftRows, extractedRightRows);
        break;
    }

    PCollection<BeamRow> ret = joinedRows
        .apply(stageName + "_JoinParts2WholeRow",
            MapElements.via(new BeamJoinTransforms.JoinParts2WholeRow()))
        .setCoder(new BeamRowCoder(CalciteUtils.toBeamRowType(getRowType())));
    return ret;
  }

  public PCollection<BeamRow> sideInputJoin(
      PCollection<KV<BeamRow, BeamRow>> extractedLeftRows,
      PCollection<KV<BeamRow, BeamRow>> extractedRightRows,
      BeamRow leftNullRow, BeamRow rightNullRow) {
    // we always make the Unbounded table on the left to do the sideInput join
    // (will convert the result accordingly before return)
    boolean swapped = (extractedLeftRows.isBounded() == PCollection.IsBounded.BOUNDED);
    JoinRelType realJoinType =
        (swapped && joinType != JoinRelType.INNER) ? JoinRelType.LEFT : joinType;

    PCollection<KV<BeamRow, BeamRow>> realLeftRows =
        swapped ? extractedRightRows : extractedLeftRows;
    PCollection<KV<BeamRow, BeamRow>> realRightRows =
        swapped ? extractedLeftRows : extractedRightRows;
    BeamRow realRightNullRow = swapped ? leftNullRow : rightNullRow;

    // swapped still need to pass down because, we need to swap the result back.
    return sideInputJoinHelper(realJoinType, realLeftRows, realRightRows,
        realRightNullRow, swapped);
  }

  private PCollection<BeamRow> sideInputJoinHelper(
      JoinRelType joinType,
      PCollection<KV<BeamRow, BeamRow>> leftRows,
      PCollection<KV<BeamRow, BeamRow>> rightRows,
      BeamRow rightNullRow, boolean swapped) {
    final PCollectionView<Map<BeamRow, Iterable<BeamRow>>> rowsView = rightRows
        .apply(View.<BeamRow, BeamRow>asMultimap());

    PCollection<BeamRow> ret = leftRows
        .apply(ParDo.of(new BeamJoinTransforms.SideInputJoinDoFn(
            joinType, rightNullRow, rowsView, swapped)).withSideInputs(rowsView))
        .setCoder(new BeamRowCoder(CalciteUtils.toBeamRowType(getRowType())));

    return ret;
  }

  private BeamRow buildNullRow(BeamRelNode relNode) {
    BeamRowType leftType = CalciteUtils.toBeamRowType(relNode.getRowType());
    BeamRow nullRow = new BeamRow(leftType);
    for (int i = 0; i < leftType.size(); i++) {
      nullRow.addField(i, null);
    }
    return nullRow;
  }

  private List<Pair<Integer, Integer>> extractJoinColumns(int leftRowColumnCount) {
    // it's a CROSS JOIN because: condition == true
    if (condition instanceof RexLiteral && (Boolean) ((RexLiteral) condition).getValue()) {
      throw new UnsupportedOperationException("CROSS JOIN is not supported!");
    }

    RexCall call = (RexCall) condition;
    List<Pair<Integer, Integer>> pairs = new ArrayList<>();
    if ("AND".equals(call.getOperator().getName())) {
      List<RexNode> operands = call.getOperands();
      for (RexNode rexNode : operands) {
        Pair<Integer, Integer> pair = extractOneJoinColumn((RexCall) rexNode, leftRowColumnCount);
        pairs.add(pair);
      }
    } else if ("=".equals(call.getOperator().getName())) {
      pairs.add(extractOneJoinColumn(call, leftRowColumnCount));
    } else {
      throw new UnsupportedOperationException(
          "Operator " + call.getOperator().getName() + " is not supported in join condition");
    }

    return pairs;
  }

  private Pair<Integer, Integer> extractOneJoinColumn(RexCall oneCondition,
      int leftRowColumnCount) {
    List<RexNode> operands = oneCondition.getOperands();
    final int leftIndex = Math.min(((RexInputRef) operands.get(0)).getIndex(),
        ((RexInputRef) operands.get(1)).getIndex());

    final int rightIndex1 = Math.max(((RexInputRef) operands.get(0)).getIndex(),
        ((RexInputRef) operands.get(1)).getIndex());
    final int rightIndex = rightIndex1 - leftRowColumnCount;

    return new Pair<>(leftIndex, rightIndex);
  }
}
