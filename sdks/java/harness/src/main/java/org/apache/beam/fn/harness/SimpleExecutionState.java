package org.apache.beam.fn.harness;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;

public class SimpleExecutionState extends ExecutionState {
  // TODO add labels.
  private long totalMillis = 0;
  private HashMap<String, String> labelsMetadata;

  public SimpleExecutionState(String stateName, HashMap<String, String> labelsMetadata) {
    super(stateName);
    this.labelsMetadata = labelsMetadata;
  }

  public Map<String, String> getLabels () {
    return Collections.unmodifiableMap(labelsMetadata);
  }

  @Override
  public void takeSample(long millisSinceLastSample) {
    this.totalMillis += millisSinceLastSample;
  }

  public long getTotalMillis() {
    return totalMillis;
  }

  @Override
  public void reportLull(Thread trackedThread, long millis) {
    // Noop
  }
}
