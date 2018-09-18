package org.apache.beam.runners.dataflow;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.Context;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DataflowPTransformMatchers}. */
@RunWith(JUnit4.class)
public class DataflowPTransformMatchersTest {

  /**
   * Test the cases that the matcher should successfully match against. In this case, it should
   * match against {@link Combine.GroupedValues} on their own and as part of an expanded {@link
   * Combine.PerKey} transform.
   */
  @Test
  public void combineValuesWithoutSideInputsSuccessfulMatches() {
    PTransformMatcher matcher =
        new DataflowPTransformMatchers.CombineValuesWithoutSideInputsPTransformMatcher();
    AppliedPTransform<?, ?, ?> groupedValues;

    groupedValues = getCombineGroupedValuesFrom(combineGroupedValuesPipeline());
    assertThat(matcher.matches(groupedValues), is(true));

    groupedValues = getCombineGroupedValuesFrom(combinePerKeyPipeline());
    assertThat(matcher.matches(groupedValues), is(true));
  }

  /**
   * Test significant cases that the matcher should not match against. In this case, this explicitly
   * tests that any {@link Combine.GroupedValues} with side inputs should not match.
   */
  @Test
  public void combineValuesWithoutSideInputsSkipsNonmatching() {
    PTransformMatcher matcher =
        new DataflowPTransformMatchers.CombineValuesWithoutSideInputsPTransformMatcher();
    AppliedPTransform<?, ?, ?> groupedValues;

    groupedValues = getCombineGroupedValuesFrom(combineGroupedValuesWithSideInputsPipeline());
    assertThat(matcher.matches(groupedValues), is(false));

    groupedValues = getCombineGroupedValuesFrom(combinePerKeyWithSideInputsPipeline());
    assertThat(matcher.matches(groupedValues), is(false));
  }

  /**
   * Test the cases that the matcher should successfully match against. In this case, it should
   * match against {@link Combine.GroupedValues} that are part of an expanded {@link Combine.PerKey}
   * transform.
   */
  @Test
  public void combineValuesWithParentCheckSuccessfulMatches() {
    PTransformMatcher matcher =
        new DataflowPTransformMatchers.CombineValuesWithParentCheckPTransformMatcher();
    AppliedPTransform<?, ?, ?> groupedValues;

    groupedValues = getCombineGroupedValuesFrom(combinePerKeyPipeline());
    assertThat(matcher.matches(groupedValues), is(true));
  }

  /**
   * Test significant cases that the matcher should not match against. In this case, this tests that
   * any {@link Combine.GroupedValues} with side inputs should not match, and that a {@link
   * Combine.GroupedValues} without an encompassing {@link Combine.PerKey} will not match.
   */
  @Test
  public void combineValuesWithParentCheckSkipsNonmatching() {
    PTransformMatcher matcher =
        new DataflowPTransformMatchers.CombineValuesWithParentCheckPTransformMatcher();
    AppliedPTransform<?, ?, ?> groupedValues;

    groupedValues = getCombineGroupedValuesFrom(combineGroupedValuesPipeline());
    assertThat(matcher.matches(groupedValues), is(false));

    groupedValues = getCombineGroupedValuesFrom(combineGroupedValuesWithSideInputsPipeline());
    assertThat(matcher.matches(groupedValues), is(false));

    groupedValues = getCombineGroupedValuesFrom(combinePerKeyWithSideInputsPipeline());
    assertThat(matcher.matches(groupedValues), is(false));
  }

  /** Creates a simple pipeline with a {@link Combine.PerKey}. */
  private static TestPipeline combinePerKeyPipeline() {
    TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
    PCollection<KV<String, Integer>> input =
        pipeline
            .apply(Create.of(KV.of("key", 1)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));
    input.apply(Combine.perKey(new SumCombineFn()));

    return pipeline;
  }

  /** Creates a simple pipeline with a {@link Combine.PerKey} with side inputs. */
  private static TestPipeline combinePerKeyWithSideInputsPipeline() {
    TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
    PCollection<KV<String, Integer>> input =
        pipeline
            .apply(Create.of(KV.of("key", 1)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));
    PCollection<String> sideInput = pipeline.apply(Create.of("side input"));
    PCollectionView<String> sideInputView = sideInput.apply(View.asSingleton());

    input.apply(
        Combine.<String, Integer, Integer>perKey(new SumCombineFnWithContext())
            .withSideInputs(sideInputView));

    return pipeline;
  }

  /** Creates a simple pipeline with a {@link Combine.GroupedValues}. */
  private static TestPipeline combineGroupedValuesPipeline() {
    TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
    PCollection<KV<String, Integer>> input =
        pipeline
            .apply(Create.of(KV.of("key", 1)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));
    input.apply(GroupByKey.create()).apply(Combine.groupedValues(new SumCombineFn()));

    return pipeline;
  }

  /** Creates a simple pipeline with a {@link Combine.GroupedValues} with side inputs. */
  private static TestPipeline combineGroupedValuesWithSideInputsPipeline() {
    TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
    PCollection<KV<String, Integer>> input =
        pipeline
            .apply(Create.of(KV.of("key", 1)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));
    PCollection<String> sideInput = pipeline.apply(Create.of("side input"));
    PCollectionView<String> sideInputView = sideInput.apply(View.asSingleton());

    input
        .apply(GroupByKey.create())
        .apply(
            Combine.<String, Integer, Integer>groupedValues(new SumCombineFnWithContext())
                .withSideInputs(sideInputView));

    return pipeline;
  }

  /** Traverse the pipeline and return the first {@link Combine.GroupedValues} found. */
  private static AppliedPTransform<?, ?, ?> getCombineGroupedValuesFrom(TestPipeline pipeline) {
    final AppliedPTransform<?, ?, ?>[] transform = new AppliedPTransform<?, ?, ?>[1];
    pipeline.traverseTopologically(
        new Pipeline.PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            if (!node.isRootNode()
                && node.toAppliedPTransform(getPipeline())
                    .getTransform()
                    .getClass()
                    .equals(Combine.GroupedValues.class)) {
              transform[0] = node.toAppliedPTransform(getPipeline());
              return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }
        });
    return transform[0];
  }

  private static class SumCombineFn extends Combine.CombineFn<Integer, Integer, Integer> {
    @Override
    public Integer createAccumulator() {
      return 0;
    }

    @Override
    public Integer addInput(Integer accum, Integer input) {
      return accum + input;
    }

    @Override
    public Integer mergeAccumulators(Iterable<Integer> accumulators) {
      Integer sum = 0;
      for (Integer accum : accumulators) {
        sum += accum;
      }
      return sum;
    }

    @Override
    public Integer extractOutput(Integer accumulator) {
      return accumulator;
    }
  }

  private static class SumCombineFnWithContext
      extends CombineWithContext.CombineFnWithContext<Integer, Integer, Integer> {
    SumCombineFn delegate;

    @Override
    public Integer createAccumulator(Context c) {
      return delegate.createAccumulator();
    }

    @Override
    public Integer addInput(Integer accum, Integer input, Context c) {
      return delegate.addInput(accum, input);
    }

    @Override
    public Integer mergeAccumulators(Iterable<Integer> accumulators, Context c) {
      return delegate.mergeAccumulators(accumulators);
    }

    @Override
    public Integer extractOutput(Integer accumulator, Context c) {
      return delegate.extractOutput(accumulator);
    }
  }
}
