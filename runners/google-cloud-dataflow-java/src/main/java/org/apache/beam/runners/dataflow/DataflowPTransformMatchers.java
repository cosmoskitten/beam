package org.apache.beam.runners.dataflow;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayDeque;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * A set of {@link PTransformMatcher PTransformMatchers} that are used in the Dataflow Runner and
 * not general enough to be shared between runners.
 */
class DataflowPTransformMatchers {
  private DataflowPTransformMatchers() {}

  /**
   * Returns a {@link PTransformMatcher} that matches {@link PTransform}s of class {@link
   * Combine.GroupedValues} that will be translated into CombineValues transforms in Dataflow's Job
   * API and skips those that should be expanded into ParDos.
   *
   * @param fnApiEnabled Flag indicating whether this matcher is being retrieved for a fnapi or
   *     non-fnapi pipeline.
   */
  static PTransformMatcher combineValuesTranslation(boolean fnApiEnabled) {
    if (fnApiEnabled) {
      return new CombineValuesWithParentCheckPTransformMatcher();
    } else {
      return new CombineValuesWithoutSideInputsPTransformMatcher();
    }
  }

  /**
   * Matches {@link PTransform}s of class {@link Combine.GroupedValues} that have no side inputs.
   */
  @VisibleForTesting
  protected static class CombineValuesWithoutSideInputsPTransformMatcher
      implements PTransformMatcher {

    @Override
    public boolean matches(AppliedPTransform<?, ?, ?> application) {
      return application.getTransform().getClass().equals(Combine.GroupedValues.class)
          && ((Combine.GroupedValues<?, ?, ?>) application.getTransform())
              .getSideInputs()
              .isEmpty();
    }

    @Override
    public String toString() {
      return toStringHelper(CombineValuesWithoutSideInputsPTransformMatcher.class).toString();
    }
  }

  /**
   * Matches {@link PTransform}s of class {@link Combine.GroupedValues} that have no side inputs and
   * are direct subtransforms of a {@link Combine.PerKey}.
   */
  @VisibleForTesting
  protected static class CombineValuesWithParentCheckPTransformMatcher
      implements PTransformMatcher {

    @Override
    public boolean matches(AppliedPTransform<?, ?, ?> application) {
      return application.getTransform().getClass().equals(Combine.GroupedValues.class)
          && ((Combine.GroupedValues<?, ?, ?>) application.getTransform()).getSideInputs().isEmpty()
          && parentIsCombinePerKey(application);
    }

    private boolean parentIsCombinePerKey(AppliedPTransform<?, ?, ?> application) {
      // We want the PipelineVisitor below to change the parent, but the parent must be final to
      // be captured in there. To work around this issue, wrap the parent in a one element array.
      final TransformHierarchy.Node[] parent = new TransformHierarchy.Node[1];

      // Traverse the pipeline to find the parent transform to application. Do this by maintaining
      // a stack of each composite transform being entered, and grabbing the top transform of the
      // stack once the target node is visited.
      Pipeline pipeline = application.getPipeline();
      pipeline.traverseTopologically(
          new Pipeline.PipelineVisitor.Defaults() {
            private ArrayDeque<TransformHierarchy.Node> parents = new ArrayDeque<>();

            @Override
            public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
              CompositeBehavior behavior = CompositeBehavior.ENTER_TRANSFORM;

              // Combine.GroupedValues is a composite transform in the hierarchy, so when entering
              // a composite first we check if we found our target node.
              if (!node.isRootNode()
                  && node.toAppliedPTransform(getPipeline()).equals(application)) {
                // If we found the target node grab the node's parent.
                if (parents.isEmpty()) {
                  parent[0] = null;
                } else {
                  parent[0] = parents.peekFirst();
                }
                behavior = CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
              }

              // Even if we found the target node we must add it to the list to maintain parity
              // with the number of removeFirst calls.
              parents.addFirst(node);
              return behavior;
            }

            @Override
            public void leaveCompositeTransform(TransformHierarchy.Node node) {
              if (!node.isRootNode()) {
                parents.removeFirst();
              }
            }
          });

      if (parent[0] == null) {
        return false;
      }

      // If the parent transform cannot be converted to an appliedPTransform it's definitely not
      // a CombinePerKey.
      AppliedPTransform<?, ?, ?> appliedParent;
      try {
        appliedParent = parent[0].toAppliedPTransform(pipeline);
      } catch (NullPointerException e) {
        return false;
      }

      return appliedParent.getTransform().getClass().equals(Combine.PerKey.class);
    }

    @Override
    public String toString() {
      return toStringHelper(CombineValuesWithParentCheckPTransformMatcher.class).toString();
    }
  }
}
