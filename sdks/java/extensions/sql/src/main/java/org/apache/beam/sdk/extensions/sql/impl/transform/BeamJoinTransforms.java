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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.schema.BeamSqlRecord;
import org.apache.beam.sdk.extensions.sql.schema.BeamSqlRecordTypeProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.Pair;

/**
 * Collections of {@code PTransform} and {@code DoFn} used to perform JOIN operation.
 */
public class BeamJoinTransforms {

  /**
   * A {@code SimpleFunction} to extract join fields from the specified row.
   */
  public static class ExtractJoinFields
      extends SimpleFunction<BeamSqlRecord, KV<BeamSqlRecord, BeamSqlRecord>> {
    private final boolean isLeft;
    private final List<Pair<Integer, Integer>> joinColumns;

    public ExtractJoinFields(boolean isLeft, List<Pair<Integer, Integer>> joinColumns) {
      this.isLeft = isLeft;
      this.joinColumns = joinColumns;
    }

    @Override public KV<BeamSqlRecord, BeamSqlRecord> apply(BeamSqlRecord input) {
      // build the type
      // the name of the join field is not important
      List<String> names = new ArrayList<>(joinColumns.size());
      List<Integer> types = new ArrayList<>(joinColumns.size());
      for (int i = 0; i < joinColumns.size(); i++) {
        names.add("c" + i);
        types.add(isLeft
            ? input.getDataType().getFieldsType().get(joinColumns.get(i).getKey()) :
            input.getDataType().getFieldsType().get(joinColumns.get(i).getValue()));
      }
      BeamSqlRecordTypeProvider type = BeamSqlRecordTypeProvider.create(names, types);

      // build the row
      BeamSqlRecord row = new BeamSqlRecord(type);
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
  public static class SideInputJoinDoFn
      extends DoFn<KV<BeamSqlRecord, BeamSqlRecord>, BeamSqlRecord> {
    private final PCollectionView<Map<BeamSqlRecord, Iterable<BeamSqlRecord>>> sideInputView;
    private final JoinRelType joinType;
    private final BeamSqlRecord rightNullRow;
    private final boolean swap;

    public SideInputJoinDoFn(JoinRelType joinType, BeamSqlRecord rightNullRow,
        PCollectionView<Map<BeamSqlRecord, Iterable<BeamSqlRecord>>> sideInputView,
        boolean swap) {
      this.joinType = joinType;
      this.rightNullRow = rightNullRow;
      this.sideInputView = sideInputView;
      this.swap = swap;
    }

    @ProcessElement public void processElement(ProcessContext context) {
      BeamSqlRecord key = context.element().getKey();
      BeamSqlRecord leftRow = context.element().getValue();
      Map<BeamSqlRecord, Iterable<BeamSqlRecord>> key2Rows = context.sideInput(sideInputView);
      Iterable<BeamSqlRecord> rightRowsIterable = key2Rows.get(key);

      if (rightRowsIterable != null && rightRowsIterable.iterator().hasNext()) {
        Iterator<BeamSqlRecord> it = rightRowsIterable.iterator();
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
      extends SimpleFunction<KV<BeamSqlRecord, KV<BeamSqlRecord, BeamSqlRecord>>, BeamSqlRecord> {
    @Override public BeamSqlRecord apply(
        KV<BeamSqlRecord, KV<BeamSqlRecord, BeamSqlRecord>> input) {
      KV<BeamSqlRecord, BeamSqlRecord> parts = input.getValue();
      BeamSqlRecord leftRow = parts.getKey();
      BeamSqlRecord rightRow = parts.getValue();
      return combineTwoRowsIntoOne(leftRow, rightRow, false);
    }
  }

  /**
   * As the method name suggests: combine two rows into one wide row.
   */
  private static BeamSqlRecord combineTwoRowsIntoOne(BeamSqlRecord leftRow,
      BeamSqlRecord rightRow, boolean swap) {
    if (swap) {
      return combineTwoRowsIntoOneHelper(rightRow, leftRow);
    } else {
      return combineTwoRowsIntoOneHelper(leftRow, rightRow);
    }
  }

  /**
   * As the method name suggests: combine two rows into one wide row.
   */
  private static BeamSqlRecord combineTwoRowsIntoOneHelper(BeamSqlRecord leftRow,
      BeamSqlRecord rightRow) {
    // build the type
    List<String> names = new ArrayList<>(leftRow.size() + rightRow.size());
    names.addAll(leftRow.getDataType().getFieldsName());
    names.addAll(rightRow.getDataType().getFieldsName());

    List<Integer> types = new ArrayList<>(leftRow.size() + rightRow.size());
    types.addAll(leftRow.getDataType().getFieldsType());
    types.addAll(rightRow.getDataType().getFieldsType());
    BeamSqlRecordTypeProvider type = BeamSqlRecordTypeProvider.create(names, types);

    BeamSqlRecord row = new BeamSqlRecord(type);
    // build the row
    for (int i = 0; i < leftRow.size(); i++) {
      row.addField(i, leftRow.getFieldValue(i));
    }

    for (int i = 0; i < rightRow.size(); i++) {
      row.addField(i + leftRow.size(), rightRow.getFieldValue(i));
    }

    return row;
  }
}
