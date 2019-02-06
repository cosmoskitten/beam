package org.apache.beam.runners.core.metrics;

// TODO(ajamato): Consider putting this logic in MetricsEnvironment, after moving
// files around to prevent dependency cycles.

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

import java.io.Closeable;

/**
 * A Utility class to support set a thread local MetricContainerStepMap.
 * The thread local MetricContainerStepMap can used to set the appropriate MetricContainer
 * by downstream pCollection consumers code, without changing the FnDataReceiver interface.
 */
public class MetricsContainerStepMapEnvironment {

  private static final ThreadLocal<MetricsContainerStepMap> METRICS_CONTAINER_STEP_MAP_FOR_THREAD =
      new ThreadLocal<>();

  /**
   * Sets a MetricsContainerStepMap on the current scope.
   *
   * @return A {@link Closeable} to deactivate state sampling.
   */
  public static Closeable activate() {
    checkState(
        METRICS_CONTAINER_STEP_MAP_FOR_THREAD.get() == null,
        "MetricsContainerStepMapEnvironment is already active in current scope.");
    METRICS_CONTAINER_STEP_MAP_FOR_THREAD.set(new MetricsContainerStepMap());
    return MetricsContainerStepMapEnvironment::deactivate;
  }

  private static void deactivate() {
    METRICS_CONTAINER_STEP_MAP_FOR_THREAD.remove();
  }

  /**
   * Precondition: a MetricsContainerStepMap is activated() in the current scope.
   * @return The currently in scope thread local MetricContainerStepMap.
   */
  public static MetricsContainerStepMap getCurrent() {
    checkState(
        METRICS_CONTAINER_STEP_MAP_FOR_THREAD.get() != null,
        "MetricsContainerStepMapEnvironment is not active in current scope.");
    return METRICS_CONTAINER_STEP_MAP_FOR_THREAD.get();
  }
}
