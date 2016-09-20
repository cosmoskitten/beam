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

import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * Interface for reporting metric updates of type {@code T} from inside worker harness.
 */
@Experimental(Kind.METRICS)
interface MetricCell<T> {

  /**
   * Return the change in this metric since the last delta was committed.
   *
   * <p>If {@code includeZero} is false and there is no change, return {@code null}.
   */
  @Nullable T getDeltaUpdate(boolean includeZero);

  /**
   * Called after a value produced by {@link #getDeltaUpdate} has been reported to indicate that
   * it should not be included in a future call to {@link #getDeltaUpdate}.
   */
  void commitDeltaUpdate(T deltaUpdate);

  /**
   * Return the cumulative value of the metric.
   */
  T getCumulativeUpdate();
}
