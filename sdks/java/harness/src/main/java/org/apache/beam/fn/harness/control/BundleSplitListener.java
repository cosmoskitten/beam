package org.apache.beam.fn.harness.control;

import java.util.List;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleSplit.Application;

public interface BundleSplitListener {
  void split(String pTransformId, List<Application> primaryRoots, List<Application> residualRoots);
}
