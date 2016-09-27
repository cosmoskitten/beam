/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.metrics;

import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * Tracks the current value (and delta) for a Distribution metric.
 */
@Experimental(Kind.METRICS)
class DistributionCell implements MetricCell<DistributionData> {

  private final AtomicReference<DistributionData> value =
      new AtomicReference<DistributionData>(DistributionData.ZERO);
  private final AtomicReference<DistributionData> delta =
      new AtomicReference<DistributionData>(DistributionData.ZERO);

  /** Increment the counter by the given amount. */
  public void report(long n) {
    DistributionData increment = DistributionData.singleton(n);
    add(value, increment);
    add(delta, increment);
  }

  private static void add(AtomicReference<DistributionData> data, DistributionData delta) {
    DistributionData original;
    do {
      original = data.get();
    } while (!data.compareAndSet(original, original.add(delta)));
  }

  @Nullable
  @Override
  public DistributionData getDeltaUpdate(boolean includeZero) {
    DistributionData delta = this.delta.get();
    return delta.count() == 0 && !includeZero ? null : delta;
  }

  @Override
  public void commitDeltaUpdate(DistributionData deltaUpdate) {
    add(delta, deltaUpdate.negate());
  }

  @Override
  public DistributionData getCumulativeUpdate() {
    return value.get();
  }
}
