package org.apache.beam.runners.flink.metrics;

import org.apache.beam.sdk.metrics.MetricsContainers;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

/**
 * Accumulator of {@link MetricsContainers}.
 */
public class MetricsAccumulator implements SimpleAccumulator<MetricsContainers> {
  private MetricsContainers metricsContainers = new MetricsContainers();

  @Override public void add(MetricsContainers value) {
    metricsContainers.update(value);
  }

  @Override public MetricsContainers getLocalValue() {
    return metricsContainers;
  }

  @Override public void resetLocal() {
    metricsContainers = new MetricsContainers();
  }

  @Override public void merge(Accumulator<MetricsContainers, MetricsContainers> other) {
    this.add(other.getLocalValue());
  }

  @Override public Accumulator<MetricsContainers, MetricsContainers> clone() {
    MetricsAccumulator metricsAccumulator = new MetricsAccumulator();
    metricsAccumulator.getLocalValue().update(this.getLocalValue());
    return metricsAccumulator;
  }
}
