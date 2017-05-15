package org.apache.beam.sdk.io.gcp.datastore;

import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.MovingFunction;


class MovingAverage {
  private final MovingFunction sum;
  private final MovingFunction count;

  public MovingAverage(long samplePeriodMs, long sampleUpdateMs,
                        int numSignificantBuckets, int numSignificantSamples) {
    sum = new MovingFunction(samplePeriodMs, sampleUpdateMs,
        numSignificantBuckets, numSignificantSamples, Sum.ofLongs());
    count = new MovingFunction(samplePeriodMs, sampleUpdateMs,
        numSignificantBuckets, numSignificantSamples, Sum.ofLongs());
  }

  public void add(long nowMsSinceEpoch, long value) {
    sum.add(nowMsSinceEpoch, value);
    count.add(nowMsSinceEpoch, 1);
  }

  public long get(long nowMsSinceEpoch) {
    return sum.get(nowMsSinceEpoch) / count.get(nowMsSinceEpoch);
  }

  public boolean hasValue(long nowMsSinceEpoch) {
    return sum.isSignificant() && count.isSignificant()
      && count.get(nowMsSinceEpoch) > 0;
  }
}
