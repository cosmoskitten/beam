package org.apache.beam.runners.fnexecution.control;

/**
 * A bundle factory scoped to a particular
 * {@link org.apache.beam.runners.core.construction.graph.ExecutableStage}, which has all of the
 * resources it needs to provide new {@link RemoteBundle RemoteBundles}.
 *
 * <p>Closing a StageBundleFactory signals that a a stage has completed and any resources bound to
 * its lifetime can be cleaned up.
 */
public interface StageBundleFactory extends AutoCloseable {
  /**
   * Get a new {@link RemoteBundle bundle} for processing the data in an executable stage.
   */
  <InputT> RemoteBundle<InputT> getBundle() throws Exception;
}
