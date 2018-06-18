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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

/** BeamRelNode to replace a {@code Enumerable} node. */
public class BeamEnumerableConverter extends ConverterImpl implements EnumerableRel {
  private static final int DaemonThreadSleepIntervalMillis = 1000;

  private final PipelineOptions options = PipelineOptionsFactory.create();
  private static final ConcurrentHashMap<Long, PipelineResult> pipelineResults =
      new ConcurrentHashMap<Long, PipelineResult>();

  public BeamEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BeamEnumerableConverter(getCluster(), traitSet, sole(inputs));
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    // This should always be a last resort.
    return planner.getCostFactory().makeHugeCost();
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer prefer) {
    final BlockBuilder list = new BlockBuilder();
    final RelDataType rowType = getRowType();
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), rowType, prefer.preferArray());
    final Expression options = implementor.stash(this.options, PipelineOptions.class);
    final Expression node = implementor.stash((BeamRelNode) getInput(), BeamRelNode.class);
    list.add(Expressions.call(BeamEnumerableConverter.class, "toEnumerable", options, node));
    return implementor.result(physType, list.toBlock());
  }

  public static Enumerable<Object> toEnumerable(PipelineOptions options, BeamRelNode node) {
    final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(BeamEnumerableConverter.class.getClassLoader());
      if (node instanceof BeamIOSinkRel) {
        return count(options, node);
      } else if (isLimitQuery(node)) {
        return limitCollect(options, node);
      }

      return collect(options, node);
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  enum LimitState {
    REACHED,
    NOT_REACHED
  }

  private static class LimitStateWrapper implements Serializable {
    private LimitState state;

    public LimitStateWrapper() {
      state = LimitState.NOT_REACHED;
    }

    public void setReached() {
      state = LimitState.REACHED;
    }

    public boolean isReached() {
      return state == LimitState.REACHED;
    }
  }

  private static PipelineResult run(
      PipelineOptions options, BeamRelNode node, DoFn<Row, Void> doFn) {
    Pipeline pipeline = Pipeline.create(options);
    PCollectionTuple.empty(pipeline).apply(node.toPTransform()).apply(ParDo.of(doFn));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    return result;
  }

  private static PipelineResult limitRun(
      PipelineOptions options,
      BeamRelNode node,
      DoFn<Row, Void> doFn,
      LimitStateWrapper stateWrapper) {
    ExecutorService pool = Executors.newFixedThreadPool(1);

    options.as(DirectOptions.class).setBlockOnRun(false);
    Pipeline pipeline = Pipeline.create(options);
    PCollectionTuple.empty(pipeline).apply(node.toPTransform()).apply(ParDo.of(doFn));

    PipelineResult result = pipeline.run();

    pool.execute(
        new Runnable() {
          @Override
          public void run() {
            while (!result.getState().isTerminal()) {
              try {
                Thread.sleep(DaemonThreadSleepIntervalMillis);
                if (stateWrapper.isReached()) {
                  result.cancel();
                  break;
                }
              } catch (IOException e) {
                e.printStackTrace();
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
          }
        });
    result.waitUntilFinish();
    pool.shutdown();
    return result;
  }

  private static Enumerable<Object> collect(PipelineOptions options, BeamRelNode node) {
    long id = options.getOptionsId();
    Queue<Object> values = new ConcurrentLinkedQueue<Object>();

    checkArgument(
        options
            .getRunner()
            .getCanonicalName()
            .equals("org.apache.beam.runners.direct.DirectRunner"));
    Collector.globalValues.put(id, values);
    run(options, node, new Collector());
    Collector.globalValues.remove(id);

    return Linq4j.asEnumerable(values);
  }

  private static Enumerable<Object> limitCollect(PipelineOptions options, BeamRelNode node) {
    long id = options.getOptionsId();
    Queue<Object> values = new ConcurrentLinkedQueue<Object>();

    checkArgument(
        options
            .getRunner()
            .getCanonicalName()
            .equals("org.apache.beam.runners.direct.DirectRunner"));

    LimitStateWrapper stateWrapper = new LimitStateWrapper();
    LimitCounter.globalValues.put(id, values);
    LimitCounter.globalLimitArguments.put(id, getLimitCount(node));
    LimitCounter.globalStates.put(id, stateWrapper);
    limitRun(options, node, new LimitCounter(), stateWrapper);
    LimitCounter.globalValues.remove(id);
    LimitCounter.globalLimitArguments.remove(id);
    LimitCounter.globalStates.remove(id);

    return Linq4j.asEnumerable(values);
  }

  private static class LimitCounter extends DoFn<Row, Void> {
    private static final Map<Long, Integer> globalLimitArguments =
        new ConcurrentHashMap<Long, Integer>();
    private static final Map<Long, Queue<Object>> globalValues =
        new ConcurrentHashMap<Long, Queue<Object>>();
    private static final Map<Long, LimitStateWrapper> globalStates =
        new ConcurrentHashMap<Long, LimitStateWrapper>();

    @Nullable private volatile Queue<Object> values;
    @Nullable private volatile Integer count;
    @Nullable private volatile LimitStateWrapper stateWrapper;

    @StartBundle
    public void startBundle(StartBundleContext context) {
      long id = context.getPipelineOptions().getOptionsId();
      values = globalValues.get(id);
      count = globalLimitArguments.get(id);
      stateWrapper = globalStates.get(id);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      Object[] input = context.element().getValues().toArray();

      if (values.size() < count) {
        if (input.length == 1) {
          values.add(input[0]);
        } else {
          values.add(input);
        }
      }

      if (values.size() >= count && !stateWrapper.isReached()) {
        stateWrapper.setReached();
      }
    }
  }

  private static class Collector extends DoFn<Row, Void> {
    // This will only work on the direct runner.
    private static final Map<Long, Queue<Object>> globalValues =
        new ConcurrentHashMap<Long, Queue<Object>>();

    @Nullable private volatile Queue<Object> values;

    @StartBundle
    public void startBundle(StartBundleContext context) {
      long id = context.getPipelineOptions().getOptionsId();
      values = globalValues.get(id);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      Object[] input = context.element().getValues().toArray();
      if (input.length == 1) {
        values.add(input[0]);
      } else {
        values.add(input);
      }
    }
  }

  private static Enumerable<Object> count(PipelineOptions options, BeamRelNode node) {
    PipelineResult result = run(options, node, new RowCounter());
    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(BeamEnumerableConverter.class, "rows"))
                    .build());
    long count = metrics.getCounters().iterator().next().getAttempted();
    return Linq4j.singletonEnumerable(count);
  }

  private static class RowCounter extends DoFn<Row, Void> {
    final Counter rows = Metrics.counter(BeamEnumerableConverter.class, "rows");

    @ProcessElement
    public void processElement(ProcessContext context) {
      rows.inc();
    }
  }

  private static boolean isLimitQuery(BeamRelNode node) {
    return (node instanceof BeamSortRel && ((BeamSortRel) node).isLimitOnly())
        || (node instanceof BeamCalcRel && ((BeamCalcRel) node).isInputSortRelAndLimitOnly());
  }

  private static int getLimitCount(BeamRelNode node) {
    if (node instanceof BeamSortRel) {
      return ((BeamSortRel) node).getCount();
    } else if (node instanceof BeamCalcRel) {
      return ((BeamCalcRel) node).getLimitCountOfSortRel();
    }

    throw new RuntimeException(
        "Cannot get limit count from RelNode tree with root " + node.getRelTypeName());
  }
}
