package org.apache.beam.runners.fnexecution.control;

import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * A bundle capable of handling input data elements for a
 * {@link org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor bundle descriptor}
 * by forwarding them to a remote environment for processing.
 *
 * <p>When a RemoteBundle is closed, it will block until bundle processing is finished on remote
 * resources, and throw an exception if bundle processing has failed.
 */
public interface RemoteBundle<InputT> extends AutoCloseable {
  /**
   * Get an id used to represent this bundle.
   */
  String getBundleId();

  /**
   * Get a {@link FnDataReceiver receiver} which consumes input elements, forwarding them to the
   * remote environment.
   */
  FnDataReceiver<WindowedValue<InputT>> getInputReceiver();
}
