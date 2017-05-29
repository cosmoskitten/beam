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

package org.apache.beam.dsls.sql.transform;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.beam.dsls.sql.schema.BeamSQLRecordType;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

/**
 * Collections of {@code PTransform} and {@code DoFn} used to perform JOIN operation.
 */
public class BeamJoinTransforms {

  /**
   * A {@code SimpleFunction} to extract join fields from the specified row.
   */
  public static class ExtractJoinFields
      extends SimpleFunction<BeamSQLRow, KV<BeamSQLRow, BeamSQLRow>> {
    private boolean isLeft;
    private List<Pair<Integer, Integer>> joinColumns;

    public ExtractJoinFields(boolean isLeft, List<Pair<Integer, Integer>> joinColumns) {
      this.isLeft = isLeft;
      this.joinColumns = joinColumns;
    }

    @Override public KV<BeamSQLRow, BeamSQLRow> apply(BeamSQLRow input) {
      BeamSQLRecordType type = new BeamSQLRecordType();
      // build the type
      // the name of the join field is not important
      for (int i = 0; i < joinColumns.size(); i++) {
        type.addField("c" + i, isLeft
            ? input.getDataType().getFieldsType().get(joinColumns.get(i).getKey()) :
            input.getDataType().getFieldsType().get(joinColumns.get(i).getValue()));
      }

      // build the row
      BeamSQLRow row = new BeamSQLRow(type);
      for (int i = 0; i < joinColumns.size(); i++) {
        row.addField(i, input
            .getFieldValue(isLeft ? joinColumns.get(i).getKey() : joinColumns.get(i).getValue()));
      }
      return KV.of(row, input);
    }
  }


  /**
   * A {@code DoFn} which implement the sideInput-JOIN.
   */
  public static class SideInputJoinDoFn extends DoFn<KV<BeamSQLRow, BeamSQLRow>, BeamSQLRow> {
    private PCollectionView<Map<BeamSQLRow, Iterable<BeamSQLRow>>> sideInputView;
    private JoinRelType joinType;
    private BeamSQLRow rightNullRow;
    private boolean swap;

    public SideInputJoinDoFn(JoinRelType joinType, BeamSQLRow rightNullRow,
        PCollectionView<Map<BeamSQLRow, Iterable<BeamSQLRow>>> sideInputView,
        boolean swap) {
      this.joinType = joinType;
      this.rightNullRow = rightNullRow;
      this.sideInputView = sideInputView;
      this.swap = swap;
    }

    @ProcessElement public void processElement(ProcessContext context) {
      BeamSQLRow key = context.element().getKey();
      BeamSQLRow leftRow = context.element().getValue();
      Map<BeamSQLRow, Iterable<BeamSQLRow>> key2Rows = context.sideInput(sideInputView);
      Iterable<BeamSQLRow> rightRowsIterable = key2Rows.get(key);

      if (rightRowsIterable != null && rightRowsIterable.iterator().hasNext()) {
        Iterator<BeamSQLRow> it = rightRowsIterable.iterator();
        while (it.hasNext()) {
          context.output(combineTwoRowsIntoOne(leftRow, it.next(), swap));
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
      extends SimpleFunction<KV<BeamSQLRow, KV<BeamSQLRow, BeamSQLRow>>, BeamSQLRow> {
    @Override public BeamSQLRow apply(KV<BeamSQLRow, KV<BeamSQLRow, BeamSQLRow>> input) {
      KV<BeamSQLRow, BeamSQLRow> parts = input.getValue();
      BeamSQLRow leftRow = parts.getKey();
      BeamSQLRow rightRow = parts.getValue();
      return combineTwoRowsIntoOne(leftRow, rightRow);
    }
  }

  private static BeamSQLRow combineTwoRowsIntoOne(BeamSQLRow leftRow, BeamSQLRow rightRow) {
    return combineTwoRowsIntoOne(leftRow, rightRow, false);
  }

  /**
   * As the method name suggests: combine two rows into one wide row.
   */
  private static BeamSQLRow combineTwoRowsIntoOne(BeamSQLRow leftRow,
      BeamSQLRow rightRow, boolean swap) {
    // build the type
    BeamSQLRecordType type = new BeamSQLRecordType();
    List<String> names =
        swap ? rightRow.getDataType().getFieldsName() : leftRow.getDataType().getFieldsName();
    List<SqlTypeName> types =
        swap ? rightRow.getDataType().getFieldsType() : leftRow.getDataType().getFieldsType();
    for (int i = 0; i < names.size(); i++) {
      type.addField(names.get(i), types.get(i));
    }

    names = swap ? leftRow.getDataType().getFieldsName() : rightRow.getDataType().getFieldsName();
    types = swap ? leftRow.getDataType().getFieldsType() : rightRow.getDataType().getFieldsType();
    for (int i = 0; i < names.size(); i++) {
      type.addField(names.get(i), types.get(i));
    }

    BeamSQLRow row = new BeamSQLRow(type);
    BeamSQLRow currentRow = swap ? rightRow : leftRow;
    int leftRowSize = currentRow.size();
    // build the row
    for (int i = 0; i < currentRow.size(); i++) {
      row.addField(i, currentRow.getFieldValue(i));
    }

    currentRow = swap ? leftRow : rightRow;
    for (int i = 0; i < currentRow.size(); i++) {
      row.addField(i + leftRowSize, currentRow.getFieldValue(i));
    }

    return row;
  }
}
