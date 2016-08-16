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

package org.apache.beam.sdk.testing;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.TimestampedValue;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.List;

/**
 * A testing input that generates an unbounded {@link PCollection} of elements, advancing the
 * watermark and processing time as elements are emitted. After all of the specified elements are
 * emitted, ceases to produce output.
 *
 * <p>Each call to a {@link TestStream.Builder} method will only be reflected in the state of the
 * {@link Pipeline} after each method before it has completed and no more progress can be made by
 * the {@link Pipeline}. A {@link PipelineRunner} must ensure that no more progress can be made in
 * the {@link Pipeline} before advancing the state of the {@link TestStream}.
 */
public class TestStream<T> extends PTransform<PBegin, PCollection<T>> {
  private final List<TestSourceEvent<T>> events;
  private final Coder<T> coder;

  /**
   * Create a new {@link TestStream.Builder} with no elements and watermark equal to
   * {@link BoundedWindow#TIMESTAMP_MIN_VALUE}.
   */
  public static <T> Builder<T> create(Coder<T> coder) {
    return new Builder<>(coder);
  }

  private TestStream(
      Coder<T> coder,
      List<TestSourceEvent<T>> events) {
    this.coder = coder;
    this.events = checkNotNull(events);
  }

  /**
   * An incomplete {@link TestStream}. Elements added to this builder will be produced in
   * sequence when the pipeline created by the {@link TestStream} is run.
   */
  public static class Builder<T> {
    private final Coder<T> coder;
    private final ImmutableList.Builder<TestSourceEvent<T>> events;
    private Instant currentWatermark;

    private Builder(Coder<T> coder) {
      this.coder = coder;
      events = ImmutableList.builder();

      currentWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    /**
     * Adds the specified elements to the source with timestamp equal to the current watermark.
     *
     * @return this {@link TestStream.Builder}
     */
    @SafeVarargs
    public final Builder<T> addElements(
        T element, T... elements) {
      TimestampedValue<T> firstElement = TimestampedValue.of(element, currentWatermark);
      @SuppressWarnings("unchecked")
      TimestampedValue<T>[] remainingElements = new TimestampedValue[elements.length];
      for (int i = 0; i < elements.length; i++) {
        remainingElements[i] = TimestampedValue.of(elements[i], currentWatermark);
      }
      return addElements(firstElement, remainingElements);
    }

    /**
     * Adds the specified elements to the source with the provided timestamps.
     *
     * @return this {@link TestStream.Builder}
     */
    @SafeVarargs
    public final Builder<T> addElements(
        TimestampedValue<T> element, TimestampedValue<T>... elements) {
      events.add(ElementEvent.add(element, elements));
      return this;
    }

    /**
     * Advance the watermark of this source to the specified instant.
     *
     * <p>The watermark must advance monotonically and to at most
     * {@link BoundedWindow#TIMESTAMP_MAX_VALUE}.
     *
     * @return this {@link TestStream.Builder}
     */
    public Builder<T> advanceWatermarkTo(Instant newWatermark) {
      checkArgument(newWatermark.isAfter(currentWatermark),
          "The watermark must monotonically advance");
      checkArgument(newWatermark.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE),
          "The Watermark cannot progress beyond the maximum. Got: %s. Maximum: %s",
          newWatermark,
          BoundedWindow.TIMESTAMP_MAX_VALUE);
      events.add(WatermarkEvent.<T>advanceTo(newWatermark));
      currentWatermark = newWatermark;
      return this;
    }

    /**
     * Advance the processing time by the specified amount.
     *
     * @return this {@link TestStream.Builder}
     */
    public Builder<T> advanceProcessingTime(Duration amount) {
      checkArgument(amount.getMillis() > 0,
          "Must advance the processing time by a positive amount. Got: ",
          amount);
      events.add(ProcessingTimeEvent.<T>advanceBy(amount));
      return this;
    }

    /**
     * Advance the watermark to infinity, completing this {@link TestStream}. Future calls to
     * the same builder will not affect the returned {@link TestStream}.
     */
    public TestStream<T> advanceWatermarkToInfinity() {
      events.add(WatermarkEvent.<T>advanceTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
      return new TestStream<>(coder, events.build());
    }
  }


  /**
   * An event in a {@link TestStream}. A marker interface for all events that happen while
   * evaluating a {@link TestStream}.
   */
  public interface TestSourceEvent<T> {}


  /**
   * A {@link TestSourceEvent} that produces elements.
   */
  @AutoValue
  public abstract static class ElementEvent<T> implements TestSourceEvent<T> {
    public abstract Iterable<TimestampedValue<T>> getElements();

    static <T> TestSourceEvent<T> add(
        TimestampedValue<T> element, TimestampedValue<T>[] elements) {
      return new AutoValue_TestStream_ElementEvent<>(ImmutableList
          .<TimestampedValue<T>>builder()
          .add(element)
          .add(elements)
          .build());
    }

  }


  /**
   * A {@link TestSourceEvent} that advances the watermark.
   */
  @AutoValue
  public abstract static class WatermarkEvent<T> implements TestSourceEvent<T> {
    public abstract Instant getWatermark();

    static <T> TestSourceEvent<T> advanceTo(Instant newWatermark) {
      return new AutoValue_TestStream_WatermarkEvent<>(newWatermark);
    }
  }


  /**
   * A {@link TestSourceEvent} that advances the processing time clock.
   */
  @AutoValue
  public abstract static class ProcessingTimeEvent<T> implements TestSourceEvent<T> {
    public abstract Duration getProcessingTimeAdvance();

    static <T> TestSourceEvent<T> advanceBy(Duration amount) {
      return new AutoValue_TestStream_ProcessingTimeEvent<>(amount);
    }
  }

  @Override
  public PCollection<T> apply(PBegin input) {
    return PCollection.<T>createPrimitiveOutputInternal(input.getPipeline(),
        WindowingStrategy.globalDefault(),
        IsBounded.UNBOUNDED).setCoder(coder);
  }

  public List<TestSourceEvent<T>> getStreamEvents() {
    return events;
  }
}

