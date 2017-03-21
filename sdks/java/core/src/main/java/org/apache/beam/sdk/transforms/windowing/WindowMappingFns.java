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

package org.apache.beam.sdk.transforms.windowing;

import com.google.auto.value.AutoValue;
import org.joda.time.Duration;

/**
 */
public class WindowMappingFns {
  public static WindowMappingFn<?> fromKnownFn(WindowFn<?, ?> fn) {
    if (fn instanceof GlobalWindows) {
      return global();
    } else if (fn instanceof PartitioningWindowFn) {
      return partitionedWindows((PartitioningWindowFn<?, ?>) fn);
    } else if (fn instanceof SlidingWindows) {
      return slidingWindows((SlidingWindows) fn);
    } else if (fn instanceof Sessions) {
      throw new IllegalArgumentException(
          String.format(
              "%s is not permitted as a known %s",
              Sessions.class.getSimpleName(), WindowMappingFn.class.getSimpleName()));
    }
    throw new IllegalArgumentException(
        String.format("Unknown %s %s", WindowFn.class.getSimpleName(), fn.getClass().getName()));
  }

  public static WindowMappingFn<GlobalWindow> global() {
    return new AutoValue_WindowMappingFns_GlobalWindowMappingFn();
  }

  public static <W extends BoundedWindow> WindowMappingFn<W> partitionedWindows(
      PartitioningWindowFn<?, W> windowFn) {
    return new AutoValue_WindowMappingFns_PartitioningMappingFn<>(windowFn);
  }

  public static WindowMappingFn<IntervalWindow> slidingWindows(SlidingWindows windowFn) {
    return new AutoValue_WindowMappingFns_SlidingWindowMappingFn(windowFn);
  }

  @AutoValue
  abstract static class GlobalWindowMappingFn extends WindowMappingFn<GlobalWindow> {
    @Override
    public GlobalWindow getSideInputWindow(BoundedWindow mainWindow) {
      return GlobalWindow.INSTANCE;
    }

    @Override
    public Duration maximumLookback() {
      return Duration.ZERO;
    }
  }

  @AutoValue
  abstract static class PartitioningMappingFn<W extends BoundedWindow> extends WindowMappingFn<W> {
    abstract PartitioningWindowFn<?, W> getWindowFn();

    @Override
    public W getSideInputWindow(BoundedWindow mainWindow) {
      return getWindowFn().assignWindow(mainWindow.maxTimestamp());
    }

    @Override
    public Duration maximumLookback() {
      return Duration.ZERO;
    }
  }

  @AutoValue
  abstract static class SlidingWindowMappingFn extends WindowMappingFn<IntervalWindow> {
    abstract SlidingWindows getWindowFn();

    @Override
    public IntervalWindow getSideInputWindow(BoundedWindow mainWindow) {
      IntervalWindow latest = null;
      for (IntervalWindow assigned : getWindowFn().assignWindows(mainWindow.maxTimestamp())) {
        if (latest == null || assigned.maxTimestamp().isAfter(latest.maxTimestamp())) {
          latest = assigned;
        }
      }
      return latest;
    }

    @Override
    public Duration maximumLookback() {
      return Duration.ZERO;
    }
  }
}
