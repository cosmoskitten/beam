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

package org.apache.beam.sdk.extensions.sql.impl.transform;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.BeamRowSqlType;
import org.apache.beam.sdk.extensions.sql.BeamSqlRecordHelper;
import org.apache.beam.sdk.extensions.sql.BeamSqlSeekableTable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.BeamRow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

/**
 * Collections of {@code PTransform} and {@code DoFn} used to perform JOIN operation.
 */
public class BeamJoinTransforms {

  /**
   * A {@code SimpleFunction} to extract join fields from the specified row.
   */
  public static class ExtractJoinFields
      extends SimpleFunction<BeamRow, KV<BeamRow, BeamRow>> {
    private final boolean isLeft;
    private final List<Pair<Integer, Integer>> joinColumns;

    public ExtractJoinFields(boolean isLeft, List<Pair<Integer, Integer>> joinColumns) {
      this.isLeft = isLeft;
      this.joinColumns = joinColumns;
    }

    @Override public KV<BeamRow, BeamRow> apply(BeamRow input) {
      // build the type
      // the name of the join field is not important
      List<String> names = new ArrayList<>(joinColumns.size());
      List<Integer> types = new ArrayList<>(joinColumns.size());
      for (int i = 0; i < joinColumns.size(); i++) {
        names.add("c" + i);
        types.add(isLeft
            ? BeamSqlRecordHelper.getSqlRecordType(input).getFieldTypeByIndex(
                joinColumns.get(i).getKey())
            : BeamSqlRecordHelper.getSqlRecordType(input).getFieldTypeByIndex(
                joinColumns.get(i).getValue()));
      }
      BeamRowSqlType type = BeamRowSqlType.create(names, types);

      // build the row
      List<Object> fieldValues = new ArrayList<>(joinColumns.size());
      for (Pair<Integer, Integer> joinColumn : joinColumns) {
        fieldValues.add(input
                .getFieldValue(isLeft ? joinColumn.getKey() : joinColumn.getValue()));
      }
      return KV.of(new BeamRow(type, fieldValues), input);
    }
  }


  /**
   * A {@code DoFn} which implement the sideInput-JOIN.
   */
  public static class SideInputJoinDoFn extends DoFn<KV<BeamRow, BeamRow>, BeamRow> {
    private final PCollectionView<Map<BeamRow, Iterable<BeamRow>>> sideInputView;
    private final JoinRelType joinType;
    private final BeamRow rightNullRow;
    private final boolean swap;

    public SideInputJoinDoFn(JoinRelType joinType, BeamRow rightNullRow,
        PCollectionView<Map<BeamRow, Iterable<BeamRow>>> sideInputView,
        boolean swap) {
      this.joinType = joinType;
      this.rightNullRow = rightNullRow;
      this.sideInputView = sideInputView;
      this.swap = swap;
    }

    @ProcessElement public void processElement(ProcessContext context) {
      BeamRow key = context.element().getKey();
      BeamRow leftRow = context.element().getValue();
      Map<BeamRow, Iterable<BeamRow>> key2Rows = context.sideInput(sideInputView);
      Iterable<BeamRow> rightRowsIterable = key2Rows.get(key);

      if (rightRowsIterable != null && rightRowsIterable.iterator().hasNext()) {
        for (BeamRow aRightRowsIterable : rightRowsIterable) {
          context.output(combineTwoRowsIntoOne(leftRow, aRightRowsIterable, swap));
        }
      } else {
        if (joinType == JoinRelType.LEFT) {
          context.output(combineTwoRowsIntoOne(leftRow, rightNullRow, swap));
        }
      }
    }
  }


  /**
   * A {@code SimpleFunction} to combine two rows into one.
   */
  public static class JoinParts2WholeRow
      extends SimpleFunction<KV<BeamRow, KV<BeamRow, BeamRow>>, BeamRow> {
    @Override public BeamRow apply(KV<BeamRow, KV<BeamRow, BeamRow>> input) {
      KV<BeamRow, BeamRow> parts = input.getValue();
      BeamRow leftRow = parts.getKey();
      BeamRow rightRow = parts.getValue();
      return combineTwoRowsIntoOne(leftRow, rightRow, false);
    }
  }

  /**
   * As the method name suggests: combine two rows into one wide row.
   */
  private static BeamRow combineTwoRowsIntoOne(BeamRow leftRow,
      BeamRow rightRow, boolean swap) {
    if (swap) {
      return combineTwoRowsIntoOneHelper(rightRow, leftRow);
    } else {
      return combineTwoRowsIntoOneHelper(leftRow, rightRow);
    }
  }

  /**
   * As the method name suggests: combine two rows into one wide row.
   */
  private static BeamRow combineTwoRowsIntoOneHelper(BeamRow leftRow,
      BeamRow rightRow) {
    // build the type
    List<String> names = new ArrayList<>(leftRow.getFieldCount() + rightRow.getFieldCount());
    names.addAll(leftRow.getDataType().getFieldNames());
    names.addAll(rightRow.getDataType().getFieldNames());

    List<Integer> types = new ArrayList<>(leftRow.getFieldCount() + rightRow.getFieldCount());
    types.addAll(BeamSqlRecordHelper.getSqlRecordType(leftRow).getFieldTypes());
    types.addAll(BeamSqlRecordHelper.getSqlRecordType(rightRow).getFieldTypes());
    BeamRowSqlType type = BeamRowSqlType.create(names, types);

    List<Object> fieldValues = new ArrayList<>(leftRow.getDataValues());
    fieldValues.addAll(rightRow.getDataValues());
    return new BeamRow(type, fieldValues);
  }

  /**
   * Transform to execute Join as Lookup.
   */
  public static class JoinAsLookup
      extends PTransform<PCollection<BeamRow>, PCollection<BeamRow>> {
//    private RexNode joinCondition;
    BeamSqlSeekableTable seekableTable;
    BeamRowSqlType lkpRowType;
//    int factTableColSize = 0; // TODO
    BeamRowSqlType joinSubsetType;
    List<Integer> factJoinIdx;

    public JoinAsLookup(RexNode joinCondition, BeamSqlSeekableTable seekableTable,
        BeamRowSqlType lkpRowType, int factTableColSize) {
      this.seekableTable = seekableTable;
      this.lkpRowType = lkpRowType;
      joinFieldsMapping(joinCondition, factTableColSize);
    }

    private void joinFieldsMapping(RexNode joinCondition, int factTableColSize) {
      factJoinIdx = new ArrayList<>();
      List<String> lkpJoinFieldsName = new ArrayList<>();
      List<Integer> lkpJoinFieldsType = new ArrayList<>();

      RexCall call = (RexCall) joinCondition;
      if ("AND".equals(call.getOperator().getName())) {
        List<RexNode> operands = call.getOperands();
        for (RexNode rexNode : operands) {
          factJoinIdx.add(((RexInputRef) ((RexCall) rexNode).getOperands().get(0)).getIndex());
          int lkpJoinIdx = ((RexInputRef) ((RexCall) rexNode).getOperands().get(1)).getIndex()
              - factTableColSize;
          lkpJoinFieldsName.add(lkpRowType.getFieldNameByIndex(lkpJoinIdx));
          lkpJoinFieldsType.add(lkpRowType.getFieldTypeByIndex(lkpJoinIdx));
        }
      } else if ("=".equals(call.getOperator().getName())) {
        factJoinIdx.add(((RexInputRef) call.getOperands().get(0)).getIndex());
        int lkpJoinIdx = ((RexInputRef) call.getOperands().get(1)).getIndex()
            - factTableColSize;
        lkpJoinFieldsName.add(lkpRowType.getFieldNameByIndex(lkpJoinIdx));
        lkpJoinFieldsType.add(lkpRowType.getFieldTypeByIndex(lkpJoinIdx));
      } else {
        throw new UnsupportedOperationException(
            "Operator " + call.getOperator().getName() + " is not supported in join condition");
      }

      joinSubsetType = BeamRowSqlType.create(lkpJoinFieldsName, lkpJoinFieldsType);
    }

    @Override
    public PCollection<BeamRow> expand(PCollection<BeamRow> input) {
      return input.apply("join_as_lookup", ParDo.of(new DoFn<BeamRow, BeamRow>(){
        @ProcessElement
        public void processElement(ProcessContext context) {
          BeamRow factRow = context.element();
          BeamRow joinSubRow = extractJoinSubRow(factRow);
          List<BeamRow> lookupRows = seekableTable.seekRecord(joinSubRow);
          for (BeamRow lr : lookupRows) {
            context.output(combineTwoRowsIntoOneHelper(factRow, lr));
          }
        }

        private BeamRow extractJoinSubRow(BeamRow factRow) {
          List<Object> joinSubsetValues = new ArrayList<>();
          for (int i : factJoinIdx) {
            joinSubsetValues.add(factRow.getFieldValue(i));
          }
          return new BeamRow(joinSubsetType, joinSubsetValues);
        }

      }));
    }
  }

}
