package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.Mutation;
import com.google.common.collect.Iterables;

final class MutationUtils {
  private MutationUtils() {

  }

  /**
   * Check if the mutation is a delete by a single primary key operation.
   * @param m mutation
   * @return true if mutation is a point delete
   */
  public static boolean isPointDelete(Mutation m) {
    return m.getOperation() == Mutation.Op.DELETE && Iterables.isEmpty(m.getKeySet().getRanges())
        && Iterables.size(m.getKeySet().getKeys()) == 1;
  }

}
