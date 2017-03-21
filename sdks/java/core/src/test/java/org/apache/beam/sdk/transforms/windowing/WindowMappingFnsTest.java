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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import org.apache.beam.sdk.coders.Coder;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link WindowMappingFns}.
 */
@RunWith(JUnit4.class)
public class WindowMappingFnsTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void fromKnownFns() throws Exception {
    assertThat(
        WindowMappingFns.fromKnownFn(new GlobalWindows()),
        Matchers.<WindowMappingFn<?>>equalTo(WindowMappingFns.global()));

    FixedWindows fixedWindows = FixedWindows.of(Duration.standardMinutes(10L));
    assertThat(
        WindowMappingFns.fromKnownFn(fixedWindows),
        Matchers.<WindowMappingFn<?>>equalTo(
            WindowMappingFns.partitionedWindows(fixedWindows)));

    PartitioningWindowFn<?, ?> calendarWindows = CalendarWindows.weeks(3, 2);
    assertThat(
        WindowMappingFns.fromKnownFn(calendarWindows),
        Matchers.<WindowMappingFn<?>>equalTo(WindowMappingFns.partitionedWindows(calendarWindows)));

    SlidingWindows slidingWindows =
        SlidingWindows.of(Duration.standardMinutes(3L)).every(Duration.standardSeconds(30L));
    assertThat(
        WindowMappingFns.fromKnownFn(slidingWindows),
        Matchers.<WindowMappingFn<?>>equalTo(WindowMappingFns.slidingWindows(slidingWindows)));
  }

  @Test
  public void fromKnownFnPartitioning() {

  }

  @Test
  public void fromKnownFnSessions() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(Sessions.class.getSimpleName());
    thrown.expectMessage(WindowMappingFn.class.getSimpleName());
    thrown.expectMessage("not permitted");
    WindowMappingFns.fromKnownFn(Sessions.withGapDuration(Duration.millis(1L)));
  }

  @Test
  public void fromKnownFnUnknown() throws Exception {
    WindowFn<?, ?> fn =
        new NonMergingWindowFn<Object, BoundedWindow>() {
          @Override
          public Collection<BoundedWindow> assignWindows(AssignContext c) throws Exception {
            throw new AssertionError();
          }

          @Override
          public boolean isCompatible(WindowFn<?, ?> other) {
            throw new AssertionError();
          }

          @Override
          public Coder<BoundedWindow> windowCoder() {
            throw new AssertionError();
          }

          @Override
          public BoundedWindow getSideInputWindow(BoundedWindow window) {
            throw new AssertionError();
          }
        };
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(fn.getClass().getName());
    thrown.expectMessage(WindowFn.class.getSimpleName());
    thrown.expectMessage("Unknown");
    WindowMappingFns.fromKnownFn(fn);
  }

  @Test
  public void global() throws Exception {
    WindowMappingFn<GlobalWindow> mapping = WindowMappingFns.global();
    assertThat(
        mapping.getSideInputWindow(GlobalWindow.INSTANCE),
        equalTo(GlobalWindow.INSTANCE));
    assertThat(
        mapping.getSideInputWindow(new BoundedWindow() {
          @Override
          public Instant maxTimestamp() {
            return BoundedWindow.TIMESTAMP_MIN_VALUE;
          }
        }),
        equalTo(GlobalWindow.INSTANCE));
    assertThat(
        mapping.getSideInputWindow(new IntervalWindow(new Instant(0L), new Instant(100L))),
        equalTo(GlobalWindow.INSTANCE));

    assertThat(mapping.maximumLookback(), equalTo(Duration.ZERO));
  }

  @Test
  public void partitionedWindowsFixed() throws Exception {
    PartitioningWindowFn<?, ?> windowFn = FixedWindows.of(Duration.standardMinutes(20L));
    WindowMappingFn<?> mapping = WindowMappingFns.partitionedWindows(windowFn);

    assertThat(
        mapping.getSideInputWindow(
            new BoundedWindow() {
              @Override
              public Instant maxTimestamp() {
                return new Instant(100L);
              }
            }),
        Matchers.<BoundedWindow>equalTo(
            new IntervalWindow(
                new Instant(0L), new Instant(0L).plus(Duration.standardMinutes(20L)))));
    assertThat(mapping.maximumLookback(), equalTo(Duration.ZERO));
  }

  @Test
  public void partitionedWindowsCalendar() throws Exception {
    PartitioningWindowFn<?, ?> windowFn = CalendarWindows.months(2);
    WindowMappingFn<?> mapping = WindowMappingFns.partitionedWindows(windowFn);

    assertThat(
        mapping.getSideInputWindow(
            new BoundedWindow() {
              @Override
              public Instant maxTimestamp() {
                return new Instant(100L);
              }
            }),
        equalTo(windowFn.assignWindow(new Instant(100L))));
    assertThat(mapping.maximumLookback(), equalTo(Duration.ZERO));
  }

  @Test
  public void slidingWindows() throws Exception {
    SlidingWindows windowFn =
        SlidingWindows.of(Duration.standardMinutes(20L)).every(Duration.standardMinutes(5L));
    WindowMappingFn<IntervalWindow> mapping = WindowMappingFns.slidingWindows(windowFn);

    assertThat(
        mapping.getSideInputWindow(new IntervalWindow(new Instant(0L), new Instant(100L))),
        equalTo(new IntervalWindow(new Instant(0L), Duration.standardMinutes(20L))));
    assertThat(
        mapping.getSideInputWindow(new IntervalWindow(new Instant(-100L), new Instant(0L))),
        equalTo(
            new IntervalWindow(
                new Instant(0L).minus(Duration.standardMinutes(5L)),
                Duration.standardMinutes(20L))));
    assertThat(mapping.maximumLookback(), equalTo(Duration.ZERO));
  }
}
