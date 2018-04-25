package org.apache.beam.runners.fnexecution.control;

import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;

/**
 * A factory that has all job-scoped information, and can be combined with stage-scoped information
 * to create a {@link JobBundleFactory}.
 */
public interface JobBundleFactory extends AutoCloseable {
  StageBundleFactory forStage(
      ExecutableStage executableStage, StateRequestHandler stateRequestHandler);
}
