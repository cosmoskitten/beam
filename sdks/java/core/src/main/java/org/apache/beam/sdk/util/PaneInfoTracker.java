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

import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.PaneInfoCoder;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.util.state.ReadableState;
import org.apache.beam.sdk.util.state.StateTag;
import org.apache.beam.sdk.util.state.StateTags;
import org.apache.beam.sdk.util.state.ValueState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.joda.time.Instant;

import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;

/**
 * Determine the timing and other properties of a new pane for a given computation, key and window.
 * Incorporates any previous pane, whether the pane has been produced because an
 * on-time {@link AfterWatermark} trigger firing, and the relation between the element's timestamp
 * and the current output watermark.
 */
public class PaneInfoTracker {
  @VisibleForTesting
  static final StateTag<Object, ValueState<PaneInfo>> PANE_INFO_TAG =
      StateTags.makeSystemTagInternal(StateTags.value("pane", PaneInfoCoder.INSTANCE));

  private TimerInternals timerInternals;

  public PaneInfoTracker(TimerInternals timerInternals) {
    this.timerInternals = timerInternals;
  }

  /**
   * Could we have any previous pane info stored?
   */
  public boolean hasState(ReduceFn<?, ?, ?, ?>.Context context) {
    return context.window().maxTimestamp().isBefore(GlobalWindow.INSTANCE.maxTimestamp());
  }

  /**
   * Clear any pane info state.
   */
  public void clear(ReduceFn<?, ?, ?, ?>.Context context) {
    if (hasState(context)) {
      context.state().access(PANE_INFO_TAG).clear();
    }
    // else: nothing to clear if for GlobalWindow.
  }

  /**
   * Return a ({@link ReadableState} for) the pane info appropriate for {@code context}. The pane
   * info includes the timing for the pane, who's calculation is quite subtle.
   *
   * @param isFinal should be {@code true} only if the triggering machinery can guarantee
   *                no further firings for the trigger.
   */
  public ReadableState<PaneInfo> getNextPaneInfo(
      ReduceFn<?, ?, ?, ?>.Context context, final boolean isFinal) {
    if (hasState(context)) {
      // We'll need any previous pane so as to derive the next pane's info.
      final Instant windowMaxTimestamp = context.window().maxTimestamp();
      final Object key = context.key();
      final ReadableState<PaneInfo> previousPaneFuture =
          context.state().access(PaneInfoTracker.PANE_INFO_TAG);

      return new ReadableState<PaneInfo>() {
        @Override
        public ReadableState<PaneInfo> readLater() {
          previousPaneFuture.readLater();
          return this;
        }

        @Override
        public PaneInfo read() {
          return describePane(key, windowMaxTimestamp, previousPaneFuture.read(), isFinal);
        }
      };
    } else {
      // For the GlobalWindow calculate the pane without requiring access to any previous pane.
      return new ReadableState<PaneInfo>() {
        @Override
        public ReadableState<PaneInfo> readLater() {
          // Nothing to read.
          return this;
        }

        @Override
        public PaneInfo read() {
          // Since this is for the Global window:
          //  - never first.
          //  - always early.
          //  - random nonce for speculative index.
          //  - always zero for non-speculative index.
          return PaneInfo.createPane(false, isFinal, isFinal ? Timing.ON_TIME : Timing.EARLY,
                                     ThreadLocalRandom.current().nextLong(), 0L);
        }
      };
    }
  }

  /**
   * If required store {@code currentPane} so that any successor pane can build from it.
   */
  public void storeCurrentPaneInfo(ReduceFn<?, ?, ?, ?>.Context context, PaneInfo currentPane) {
    if (hasState(context)) {
      context.state().access(PANE_INFO_TAG).write(currentPane);
    }
    // else: nothing to store for GlabalWindow.
  }

  /**
   * Derive the current pane info by combining the previous pane info and state of trigger.
   */
  private PaneInfo describePane(
      Object key, Instant windowMaxTimestamp, @Nullable PaneInfo previousPane, boolean isFinal) {
    boolean isFirst = previousPane == null;
    Timing previousTiming = isFirst ? null : previousPane.getTiming();
    long index = isFirst ? 0 : previousPane.getIndex() + 1;
    long nonSpeculativeIndex = isFirst ? 0 : previousPane.getNonSpeculativeIndex() + 1;
    Instant outputWM = timerInternals.currentOutputWatermarkTime();
    Instant inputWM = timerInternals.currentInputWatermarkTime();

    // True if it is not possible to assign the element representing this pane a timestamp
    // which will make an ON_TIME pane for any following computation.
    // Ie true if the element's latest possible timestamp is before the current output watermark.
    boolean isLateForOutput = outputWM != null && windowMaxTimestamp.isBefore(outputWM);

    // True if all emitted panes (if any) were EARLY panes.
    // Once the ON_TIME pane has fired, all following panes must be considered LATE even
    // if the output watermark is behind the end of the window.
    boolean onlyEarlyPanesSoFar = previousTiming == null || previousTiming == Timing.EARLY;

    // True is the input watermark hasn't passed the window's max timestamp.
    boolean isEarlyForInput = !inputWM.isAfter(windowMaxTimestamp);

    Timing timing;
    if (isLateForOutput || !onlyEarlyPanesSoFar) {
      // The output watermark has already passed the end of this window, or we have already
      // emitted a non-EARLY pane. Irrespective of how this pane was triggered we must
      // consider this pane LATE.
      timing = Timing.LATE;
    } else if (isEarlyForInput) {
      // This is an EARLY firing.
      timing = Timing.EARLY;
      nonSpeculativeIndex = -1;
    } else {
      // This is the unique ON_TIME firing for the window.
      timing = Timing.ON_TIME;
    }

    WindowTracing.debug(
        "describePane: {} pane (prev was {}) for key:{}; windowMaxTimestamp:{}; "
        + "inputWatermark:{}; outputWatermark:{}; isLateForOutput:{}",
        timing, previousTiming, key, windowMaxTimestamp, inputWM, outputWM, isLateForOutput);

    if (previousPane != null) {
      // Timing transitions should follow EARLY* ON_TIME? LATE*
      switch (previousTiming) {
        case EARLY:
          Preconditions.checkState(
              timing == Timing.EARLY || timing == Timing.ON_TIME || timing == Timing.LATE,
              "EARLY cannot transition to %s", timing);
          break;
        case ON_TIME:
          Preconditions.checkState(
              timing == Timing.LATE, "ON_TIME cannot transition to %s", timing);
          break;
        case LATE:
          Preconditions.checkState(timing == Timing.LATE, "LATE cannot transtion to %s", timing);
          break;
        case UNKNOWN:
          break;
      }
      Preconditions.checkState(!previousPane.isLast(), "Last pane was not last after all.");
    }

    return PaneInfo.createPane(isFirst, isFinal, timing, index, nonSpeculativeIndex);
  }
}
