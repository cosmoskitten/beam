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

import static org.apache.beam.sdk.values.PCollection.IsBounded.UNBOUNDED;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.coders.BeamRecordCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.sql.BeamRecordSqlType;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rule.AggregateWindowField;
import org.apache.beam.sdk.extensions.sql.impl.transform.BeamAggregationTransforms;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.joda.time.Duration;

/**
 * {@link BeamRelNode} to replace a {@link Aggregate} node.
 */
public class BeamAggregationRel extends Aggregate implements BeamRelNode {
  private AggregateWindowField windowField;

  public BeamAggregationRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      boolean indicator,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls,
      @Nonnull AggregateWindowField windowField) {

    super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
    this.windowField = windowField;
  }

  @Override
  public PCollection<BeamRecord> buildBeamPipeline(
      PCollectionTuple inputPCollections,
      BeamSqlEnv sqlEnv) throws Exception {

    RelNode input = getInput();
    String stageName = BeamSqlRelUtils.getStageName(this) + "_";

    PCollection<BeamRecord> upstream =
        BeamSqlRelUtils.getBeamRelInput(input).buildBeamPipeline(inputPCollections, sqlEnv);
    if (windowField.isPresent()) {
      upstream = upstream.apply(stageName + "assignEventTimestamp", WithTimestamps
          .of(new BeamAggregationTransforms.WindowTimestampFn(windowField.fieldIndex()))
          .withAllowedTimestampSkew(new Duration(Long.MAX_VALUE)))
          .setCoder(upstream.getCoder());
    }

    PCollection<BeamRecord> windowedStream = windowField.isPresent()
        ? upstream.apply(stageName + "window", Window.into(windowField.windowFn()))
        : upstream;

    validateWindowIsSupported(windowedStream);

    BeamRecordCoder keyCoder = exKeyFieldsSchema(input.getRowType()).getRecordCoder();
    PCollection<KV<BeamRecord, BeamRecord>> exCombineByStream = windowedStream.apply(
        stageName + "exCombineBy",
        WithKeys
            .of(new BeamAggregationTransforms.AggregationGroupByKeyFn(
                windowField.fieldIndex(), groupSet)))
        .setCoder(KvCoder.of(keyCoder, upstream.getCoder()));


    BeamRecordCoder aggCoder = exAggFieldsSchema().getRecordCoder();

    PCollection<KV<BeamRecord, BeamRecord>> aggregatedStream =
        exCombineByStream
            .apply(
                stageName + "combineBy",
                Combine.perKey(
                    new BeamAggregationTransforms.AggregationAdaptor(
                        getAggCallList(), CalciteUtils.toBeamRowType(input.getRowType()))))
            .setCoder(KvCoder.of(keyCoder, aggCoder));

    PCollection<BeamRecord> mergedStream = aggregatedStream.apply(
        stageName + "mergeRecord",
        ParDo.of(new BeamAggregationTransforms.MergeAggregationRecord(
            CalciteUtils.toBeamRowType(getRowType()), getAggCallList(), windowField.fieldIndex())));
    mergedStream.setCoder(CalciteUtils.toBeamRowType(getRowType()).getRecordCoder());

    return mergedStream;
  }

  private void validateWindowIsSupported(PCollection<BeamRecord> upstream) {

    // If GlobalWindows is used with unbounded input, then DefaultTrigger
    // does not produce meaningful results. Reject this case.
    if (isUnbounded(upstream)
        && isGlobalWindowing(upstream)
        && isDefaultTrigger(upstream.getWindowingStrategy())) {

      throw new UnsupportedOperationException(
          "Please explicitly specify windowing in SQL query using HOP/TUMBLE/SESSION functions "
              + "(default trigger will be used in this case). "
              + "Unbounded input with global windowing and default trigger is not supported "
              + "in Beam SQL aggregations");
    }

    // Other cases make sense with defaults, for example:
    //
    //   - input is bounded. In this case we don't care about triggering or windowing configuration.
    //     Default source configuration + whatever is specified in SQL will work;
    //
    //   - unbounded input with custom windowing was explicitly specified in SQL.
    //     In this case trigger will be inherited if specified, or default trigger will be used.
    //     Both of these cases make sense;
    //
    //   - unbounded input with global windows but non-default triggering. This is a non-default
    //     configuration, we don't know whether make sense or not;
    //
    //   - unbounded input with non-global windows should work as is;
  }

  private boolean isUnbounded(PCollection<BeamRecord> upstream) {
    return UNBOUNDED.equals(upstream.isBounded());
  }

  private boolean isGlobalWindowing(PCollection<BeamRecord> upstream) {
    return upstream.getWindowingStrategy().getWindowFn() instanceof GlobalWindows;
  }

  private boolean isDefaultTrigger(WindowingStrategy windowingStrategy) {
    return windowingStrategy.getTrigger() instanceof DefaultTrigger
        || !windowingStrategy.isTriggerSpecified();
  }

  /**
   * Type of sub-rowrecord used as Group-By keys.
   */
  private BeamRecordSqlType exKeyFieldsSchema(RelDataType relDataType) {
    BeamRecordSqlType inputRowType = CalciteUtils.toBeamRowType(relDataType);
    List<String> fieldNames = new ArrayList<>();
    List<Integer> fieldTypes = new ArrayList<>();
    for (int i : groupSet.asList()) {
      if (i != windowField.fieldIndex()) {
        fieldNames.add(inputRowType.getFieldNameByIndex(i));
        fieldTypes.add(inputRowType.getFieldTypeByIndex(i));
      }
    }
    return BeamRecordSqlType.create(fieldNames, fieldTypes);
  }

  /**
   * Type of sub-rowrecord, that represents the list of aggregation fields.
   */
  private BeamRecordSqlType exAggFieldsSchema() {
    List<String> fieldNames = new ArrayList<>();
    List<Integer> fieldTypes = new ArrayList<>();
    for (AggregateCall ac : getAggCallList()) {
      fieldNames.add(ac.name);
      fieldTypes.add(CalciteUtils.toJavaType(ac.type.getSqlTypeName()));
    }

    return BeamRecordSqlType.create(fieldNames, fieldTypes);
  }

  @Override
  public Aggregate copy(
      RelTraitSet traitSet, RelNode input, boolean indicator
      , ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    return new BeamAggregationRel(getCluster(), traitSet, input, indicator
        , groupSet, groupSets, aggCalls, windowField);
  }

  public RelWriter explainTerms(RelWriter pw) {
    // We skip the "groups" element if it is a singleton of "group".
    pw.item("group", groupSet)
        .item("window", windowField)
        .itemIf("event_time", windowField.fieldIndex(), windowField.isPresent())
        .itemIf("groups", groupSets, getGroupType() != Group.SIMPLE)
        .itemIf("indicator", indicator, indicator)
        .itemIf("aggs", aggCalls, pw.nest());
    if (!pw.nest()) {
      for (Ord<AggregateCall> ord : Ord.zip(aggCalls)) {
        pw.item(Util.first(ord.e.name, "agg#" + ord.i), ord.e);
      }
    }
    return pw;
  }

}
