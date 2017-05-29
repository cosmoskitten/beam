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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.dsls.sql.exception.BeamSqlUnsupportedException;
import org.apache.beam.dsls.sql.planner.BeamPipelineCreator;
import org.apache.beam.dsls.sql.planner.BeamSQLRelUtils;
import org.apache.beam.dsls.sql.schema.BeamSQLRecordType;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.dsls.sql.transform.BeamJoinTransforms;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
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
 * <p>For the first two cases, a standard join can be utilized to implement them as long as the
 * windowFn of the both sides match. For the third case, {@code sideInput} is utilized to implement
 * the join, hence there are some constrains for the third case: 1) FULL JOIN is not supported
 * 2) The unbounded table must be at the left side of the OUTER JOIN.
 *
 * <p>There is also some overall constrains:
 *
 * <ul>
 *  <li>Only equi-join is supported</li>
 *  <li>CROSS JOIN is not supported</li>
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

  @Override public PCollection<BeamSQLRow> buildBeamPipeline(BeamPipelineCreator planCreator)
      throws Exception {
    BeamRelNode leftRelNode = BeamSQLRelUtils.getBeamRelInput(left);
    PCollection<BeamSQLRow> leftRows = leftRelNode.buildBeamPipeline(planCreator);

    final BeamRelNode rightRelNode = BeamSQLRelUtils.getBeamRelInput(right);
    PCollection<BeamSQLRow> rightRows = rightRelNode.buildBeamPipeline(planCreator);

    String stageName = BeamSQLRelUtils.getStageName(this);
    WindowFn leftWinFn = leftRows.getWindowingStrategy().getWindowFn();
    WindowFn rightWinFn = rightRows.getWindowingStrategy().getWindowFn();

    // extract the join fields
    List<Pair<Integer, Integer>> pairs = extractJoinColumns(
        leftRelNode.getRowType().getFieldCount());

    // BeamSQLRow -> KV<BeamSQLRow, BeamSQLRow>
    PCollection<KV<BeamSQLRow, BeamSQLRow>> extractedLeftRows = leftRows
        .apply(stageName + "_left_ExtractJoinFields",
            MapElements.via(new BeamJoinTransforms.ExtractJoinFields(true, pairs)));
    PCollection<KV<BeamSQLRow, BeamSQLRow>> extractedRightRows = rightRows
        .apply(stageName + "_right_ExtractJoinFields",
            MapElements.via(new BeamJoinTransforms.ExtractJoinFields(false, pairs)));

    // prepare the NullRows
    BeamSQLRow leftNullRow = buildNullRow(leftRelNode);
    BeamSQLRow rightNullRow = buildNullRow(rightRelNode);

    // a regular join
    if (leftWinFn.isCompatible(rightWinFn)
        && ((leftRows.isBounded() == PCollection.IsBounded.BOUNDED
            && rightRows.isBounded() == PCollection.IsBounded.BOUNDED)
           || (leftRows.isBounded() == PCollection.IsBounded.UNBOUNDED
                && rightRows.isBounded() == PCollection.IsBounded.UNBOUNDED)
            )
        ) {
      return standardJoin(extractedLeftRows, extractedRightRows,
          leftNullRow, rightNullRow, stageName);
    } else if (
        (leftRows.isBounded() == PCollection.IsBounded.BOUNDED
        && rightRows.isBounded() == PCollection.IsBounded.UNBOUNDED)
        || (leftRows.isBounded() == PCollection.IsBounded.UNBOUNDED
            && rightRows.isBounded() == PCollection.IsBounded.BOUNDED)
        ) {
      // if one of the sides is Bounded & the other is Unbounded
      // then do a sideInput
      // when doing a sideInput, the windowFn does not need to match
      // Only support INNER JOIN & LEFT OUTER JOIN where left side of the join must be
      // the unbounded
      if (joinType == JoinRelType.FULL) {
        throw new BeamSqlUnsupportedException("FULL OUTER JOIN is not supported when join "
            + "a bounded table with an unbounded table.");
      }

      if ((joinType == JoinRelType.LEFT
          && leftRows.isBounded() == PCollection.IsBounded.BOUNDED)
          || (joinType == JoinRelType.RIGHT
          && rightRows.isBounded() == PCollection.IsBounded.BOUNDED)) {
        throw new BeamSqlUnsupportedException(
            "LEFT side of an OUTER JOIN must be Unbounded table.");
      }

      return sideInputJoin(extractedLeftRows, extractedRightRows,
          leftNullRow, rightNullRow);
    } else {
      throw new BeamSqlUnsupportedException(
          "The inputs to the JOIN have un-joinnable windowFns: " + leftWinFn + ", " + rightWinFn);
    }
  }

  public PCollection<BeamSQLRow> standardJoin(
      PCollection<KV<BeamSQLRow, BeamSQLRow>> extractedLeftRows,
      PCollection<KV<BeamSQLRow, BeamSQLRow>> extractedRightRows,
      BeamSQLRow leftNullRow, BeamSQLRow rightNullRow, String stageName) {
    PCollection<KV<BeamSQLRow, KV<BeamSQLRow, BeamSQLRow>>> joinedRows = null;
    switch (joinType) {
      case INNER:
        joinedRows = org.apache.beam.sdk.extensions.joinlibrary.Join
            .innerJoin(extractedLeftRows, extractedRightRows);
        break;
      case LEFT:
        joinedRows = org.apache.beam.sdk.extensions.joinlibrary.Join
            .leftOuterJoin(extractedLeftRows, extractedRightRows, rightNullRow);
        break;
      case RIGHT:
        joinedRows = org.apache.beam.sdk.extensions.joinlibrary.Join
            .rightOuterJoin(extractedLeftRows, extractedRightRows, leftNullRow);
        break;
      case FULL:
        joinedRows = fullOuterJoin(extractedLeftRows, extractedRightRows, leftNullRow,
            rightNullRow);
    }

    PCollection<BeamSQLRow> ret = joinedRows
        .apply(stageName + "_JoinParts2WholeRow",
            MapElements.via(new BeamJoinTransforms.JoinParts2WholeRow()));

    return ret;
  }

  public PCollection<BeamSQLRow> sideInputJoin(
      PCollection<KV<BeamSQLRow, BeamSQLRow>> extractedLeftRows,
      PCollection<KV<BeamSQLRow, BeamSQLRow>> extractedRightRows,
      BeamSQLRow leftNullRow, BeamSQLRow rightNullRow) {
    // if the join is not a INNER JOIN we convert the join to a left join
    // by swap the left/right side of the rows
    boolean swapped = joinType != JoinRelType.INNER
        && extractedLeftRows.isBounded() == PCollection.IsBounded.BOUNDED;

    PCollection<KV<BeamSQLRow, BeamSQLRow>> realLeftRows =
        swapped ? extractedRightRows : extractedLeftRows;
    PCollection<KV<BeamSQLRow, BeamSQLRow>> realRightRows =
        swapped ? extractedLeftRows : extractedRightRows;
    BeamSQLRow realRightNullRow = swapped ? leftNullRow : rightNullRow;
    JoinRelType realJoinType = swapped ? JoinRelType.LEFT : joinType;

    final PCollectionView<Map<BeamSQLRow, Iterable<BeamSQLRow>>> rowsView = realRightRows
        .apply(View.<BeamSQLRow, BeamSQLRow>asMultimap());
    PCollection<BeamSQLRow> ret = realLeftRows
        .apply(ParDo.of(new BeamJoinTransforms.SideInputJoinDoFn(
            realJoinType, realRightNullRow, rowsView, swapped)).withSideInputs(rowsView));

    return ret;
  }

  private BeamSQLRow buildNullRow(BeamRelNode relNode) {
    BeamSQLRecordType leftType = BeamSQLRecordType.from(relNode.getRowType());
    BeamSQLRow nullRow = new BeamSQLRow(leftType);
    for (int i = 0; i < leftType.size(); i++) {
      nullRow.addField(i, null);
    }
    return nullRow;
  }

  private List<Pair<Integer, Integer>> extractJoinColumns(int separator) {
    RexCall call = (RexCall) condition;
    List<Pair<Integer, Integer>> pairs = new ArrayList<>();
    if ("AND".equals(call.getOperator().getName())) {
      List<RexNode> operands = call.getOperands();
      for (RexNode rexNode : operands) {
        Pair<Integer, Integer> pair = extractOneJoinColumn((RexCall) rexNode, separator);
        pairs.add(pair);
      }
    } else if ("=".equals(call.getOperator().getName())) {
      pairs.add(extractOneJoinColumn(call, separator));
    } else {
      throw new BeamSqlUnsupportedException(
          "Operator " + call.getOperator().getName() + " is not supported in join condition");
    }

    return pairs;
  }

  private Pair<Integer, Integer> extractOneJoinColumn(RexCall oneCondition, int separator) {
    List<RexNode> operands = oneCondition.getOperands();
    final int leftIndex = Math.min(((RexInputRef) operands.get(0)).getIndex(),
        ((RexInputRef) operands.get(1)).getIndex());

    final int rightIndex1 = Math.max(((RexInputRef) operands.get(0)).getIndex(),
        ((RexInputRef) operands.get(1)).getIndex());
    final int rightIndex = rightIndex1 - separator;

    return new Pair<>(leftIndex, rightIndex);
  }

  // NOTE: the main piece of this method is copied from the joinlibrary
  //       currently joinlibrary does not support FULL OUTER JOIN
  //       once FULL OUTER JOIN is supported in joinlibrary we will use
  //       joinlibrary instead.
  //       Pull Request: https://github.com/apache/beam/pull/3267
  public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> fullOuterJoin(
      final PCollection<KV<K, V1>> leftCollection, final PCollection<KV<K, V2>> rightCollection,
      final V1 leftNullValue, final V2 rightNullValue) {
    checkNotNull(leftCollection);
    checkNotNull(rightCollection);
    checkNotNull(leftNullValue);
    checkNotNull(rightNullValue);

    final TupleTag<V1> v1Tuple = new TupleTag<>();
    final TupleTag<V2> v2Tuple = new TupleTag<>();

    PCollection<KV<K, CoGbkResult>> coGbkResultCollection = KeyedPCollectionTuple
        .of(v1Tuple, leftCollection).and(v2Tuple, rightCollection).apply(CoGroupByKey.<K>create());

    return coGbkResultCollection.apply(ParDo.of(new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {

      @ProcessElement public void processElement(ProcessContext c) {
        KV<K, CoGbkResult> e = c.element();

        Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
        Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);
        if (leftValuesIterable.iterator().hasNext() && rightValuesIterable.iterator().hasNext()) {
          for (V2 rightValue : rightValuesIterable) {
            for (V1 leftValue : leftValuesIterable) {
              c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
            }
          }
        } else if (leftValuesIterable.iterator().hasNext() && !rightValuesIterable.iterator()
            .hasNext()) {
          for (V1 leftValue : leftValuesIterable) {
            c.output(KV.of(e.getKey(), KV.of(leftValue, rightNullValue)));
          }
        } else if (!leftValuesIterable.iterator().hasNext() && rightValuesIterable.iterator()
            .hasNext()) {
          for (V2 rightValue : rightValuesIterable) {
            c.output(KV.of(e.getKey(), KV.of(leftNullValue, rightValue)));
          }
        }
      }
    })).setCoder(KvCoder.of(((KvCoder) leftCollection.getCoder()).getKeyCoder(), KvCoder
        .of(((KvCoder) leftCollection.getCoder()).getValueCoder(),
            ((KvCoder) rightCollection.getCoder()).getValueCoder())));
  }
}
