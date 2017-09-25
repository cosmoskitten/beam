package org.apache.beam.sdk.transforms;

import com.google.common.collect.Sets;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.apache.beam.sdk.values.PCollectionView;

public final class Requirements implements Serializable {
  private final Collection<PCollectionView<?>> sideInputs;

  private Requirements(Collection<PCollectionView<?>> sideInputs) {
    this.sideInputs = sideInputs;
  }

  public Collection<PCollectionView<?>> getSideInputs() {
    return sideInputs;
  }

  public static Requirements requiresSideInputs(Collection<PCollectionView<?>> sideInputs) {
    return new Requirements(sideInputs);
  }

  public static Requirements requiresSideInputs(PCollectionView<?>... sideInputs) {
    return requiresSideInputs(Arrays.asList(sideInputs));
  }

  public static Requirements union(Contextful<?>... elements) {
    Set<PCollectionView<?>> res = Sets.newHashSet();
    for (Contextful<?> element : elements) {
      if (element != null) {
        res.addAll(element.getRequirements().sideInputs);
      }
    }
    return new Requirements(res);
  }

  public static Requirements empty() {
    return new Requirements(Collections.<PCollectionView<?>>emptyList());
  }
}
