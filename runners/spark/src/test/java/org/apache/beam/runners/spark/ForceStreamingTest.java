package org.apache.beam.runners.spark;

import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedReadFromUnboundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.junit.Test;


/**
 * Test that we can "force streaming" on pipelines with {@link BoundedReadFromUnboundedSource}
 * inputs using the {@link TestSparkRunner}.
 *
 * <p>The test validates that when a pipeline reads from a {@link BoundedReadFromUnboundedSource},
 * with {@link SparkPipelineOptions#setStreaming(boolean)} true
 * and using the {@link TestSparkRunner}; the {@link Read.Bounded} transform
 * is replaced by an {@link Read.Unbounded} transform.
 *
 * <p>This test does not execute a pipeline.
 */
public class ForceStreamingTest {

  @Test
  public void test() throws IOException {
    SparkPipelineOptions options = PipelineOptionsFactory.create().as(SparkPipelineOptions.class);
    options.setRunner(TestSparkRunner.class);
    // force streaming.
    options.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);

    // apply the BoundedReadFromUnboundedSource.
    @SuppressWarnings("unchecked")
    BoundedReadFromUnboundedSource boundedRead =
        Read.from(new FakeUnboundedSource()).withMaxNumRecords(-1);
    //noinspection unchecked
    pipeline.apply(boundedRead);

    UnboundedReadDetector unboundedReadDetector = new UnboundedReadDetector();
    pipeline.traverseTopologically(unboundedReadDetector);

    // assert that the applied BoundedReadFromUnboundedSource
    // is being treated as an unbounded read.
    assertThat("Expected to have an unbounded read.", unboundedReadDetector.isUnbounded);
  }

  /**
   * Traverses the Pipeline to check if the input is indeed a {@link Read.Unbounded}.
   */
  private class UnboundedReadDetector extends Pipeline.PipelineVisitor.Defaults {
    private boolean isUnbounded = false;

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      Class<? extends PTransform> transformClass = node.getTransform().getClass();
      if (transformClass == Read.Unbounded.class) {
        isUnbounded = true;
      }
    }

  }

  /**
   * A fake {@link UnboundedSource} to satisfy the compiler.
   */
  private static class FakeUnboundedSource extends UnboundedSource {

    @Override
      public List<? extends UnboundedSource> generateInitialSplits(
          int desiredNumSplits,
          PipelineOptions options) throws Exception {
        return null;
      }

      @Override
      public UnboundedReader createReader(
          PipelineOptions options,
          CheckpointMark checkpointMark) throws IOException {
        return null;
      }

      @Override
      public Coder getCheckpointMarkCoder() {
        return null;
      }

      @Override
      public void validate() { }

      @Override
      public Coder getDefaultOutputCoder() {
        return null;
      }

  }

}
