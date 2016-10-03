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
 * Interface for reporting metric updates of type {@code T} from inside worker harness.
 */
@Experimental(Kind.METRICS)
abstract class MetricCell<T> {

  // All MetricCells start out Dirty so that their existence is reported.
  // When a MetricCell is updated, it transitions to the DIRTY state.
  // When a delta is extracting, they transition to the COMMITTING state.
  // When a delta is committed, it transitions to the CLEAN state only if it is in the COMMITTING
  // state. This ensures that counters that were modified after the delta was extracted but before
  // it was committed are not falsely marked as CLEAN.delta was being committed
  private enum DirtyState {
    /** Indicates that there have been changes to the MetricCell since last commit. */
    DIRTY,
    /** Indicates that there have been no changes to the MetricCell since last commit. */
    CLEAN,
    /** Indicates that a commit of the current value is in progress. */
    COMMITTING
  }

  private final AtomicReference<DirtyState> dirty = new AtomicReference<>(DirtyState.DIRTY);

  protected void markDirty() {
    dirty.set(DirtyState.DIRTY);
  }

  /**
   * Return the cumulative value of this metric if there have been any changes since the last time
   * the update was retrieved and committed.
   */
  @Nullable
  public T getUpdateIfDirty() {
    DirtyState state = dirty.get();
    if (state != DirtyState.CLEAN) {
      dirty.set(DirtyState.COMMITTING);
      return getCumulative();
    } else {
      // If the metric was clean, we know no changes have been made since the last call to
      // getUpdateIfDirty, so we can return null.
      return null;
    }
  }

  /**
   * Mark the values of the metric most recently retrieved with {@link #getUpdateIfDirty()}} as
   * committed. The next call to {@link #getUpdateIfDirty()} will return null unless there have been
   * changes made since the previous call.
   */
  public void commitUpdate() {
    dirty.compareAndSet(DirtyState.COMMITTING, DirtyState.CLEAN);
  }

  /**
   * Return the cumulative value of this metric.
   */
  public abstract T getCumulative();
}
