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
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.NonMergingWindowFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;

import org.joda.time.Instant;

import java.util.Collection;

/**
 * A {@link WindowFn} that leaves all associations between elements and windows unchanged.
 *
 * <p>In order to implement all the abstract methods of {@link WindowFn}, this requires the
 * prior {@link WindowFn}, to which all auxiliary functionality is delegated.
 *
 * <p>This {@link WindowFn} is an internal implementation detail of sdk-provided utilities, and
 * should not be used by {@link Pipeline} writers.
 */
class PassThroughWindowFn<T> extends NonMergingWindowFn<T, BoundedWindow> {

  /** The WindowFn prior to this. Used for its windowCoder, etc. */
  private final WindowFn<?, BoundedWindow> priorWindowFn;

  public PassThroughWindowFn(WindowFn<?, ?> priorWindowFn) {
    // Safe because it is only used privately here.
    // At every point where a window is returned or accepted, it has been provided
    // by priorWindowFn, so it is of the expected type.
    @SuppressWarnings("unchecked")
    WindowFn<?, BoundedWindow> internalWindowFn = (WindowFn<?, BoundedWindow>) priorWindowFn;
    this.priorWindowFn = internalWindowFn;
  }

  @Override
  public Collection<BoundedWindow> assignWindows(WindowFn<T, BoundedWindow>.AssignContext c)
      throws Exception {
    // The windows are provided by priorWindowFn, which also provides the coder for them
    @SuppressWarnings("unchecked")
    Collection<BoundedWindow> priorWindows = (Collection<BoundedWindow>) c.windows();
    return priorWindows;
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    throw new UnsupportedOperationException(
        String.format(
            "%s.isCompatible() should never be called."
                + " It is a private implementation detail of sdk utilities."
                + " This message indicates a bug in the Dataflow SDK.",
            getClass().getCanonicalName()));
  }

  @Override
  public Coder<BoundedWindow> windowCoder() {
    // Safe because priorWindowFn provides the windows also.
    // The Coder is _not_ actually a coder for an arbitrary BoundedWindow.
    return priorWindowFn.windowCoder();
  }

  @Override
  public BoundedWindow getSideInputWindow(BoundedWindow window) {
    throw new UnsupportedOperationException(
        String.format(
            "%s.getSideInputWindow() should never be called."
                + " It is a private implementation detail sdk utilities."
                + " This message indicates a bug in the Dataflow SDK.",
            getClass().getCanonicalName()));
  }

  @Deprecated
  @Override
  public Instant getOutputTime(Instant inputTimestamp, BoundedWindow window) {
    return inputTimestamp;
  }
}
