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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.annotations.Experimental;
import org.joda.time.Instant;

/**
 * A timer for a specified time domain that can be set to register the desire for
 * further processing at particular time (in some time domain).
 *
 * <p>See {@link TimeDomain} for details on the time domains available.
 *
 * <p>Timers are not guaranteed to fire synchronously, but will be delivered at some time
 * after the requested time.
 *
 * <p>An implementation of {@link Timer} is implicitly scoped - it may
 * be scoped to a key and window, or a key, window, and trigger, etc.
 *
 */
@Experimental(Experimental.Kind.TIMERS)
public interface Timer {
  /** Sets or resets the time at which this timer should fire. */
  public abstract void setTimer(Instant timestamp);

  /** Removes the timer set in this context for the {@code timestmap} and {@code timeDomain}. */
  public abstract void cancel();
}
