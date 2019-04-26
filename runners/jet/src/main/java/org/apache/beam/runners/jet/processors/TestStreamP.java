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
package org.apache.beam.runners.jet.processors;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static java.util.stream.Collectors.toList;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.beam.runners.jet.Utils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

/**
 * Jet {@link com.hazelcast.jet.core.Processor} implementation for Beam's {@link TestStream}
 * transform.
 */
public class TestStreamP extends AbstractProcessor {

  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final Traverser traverser;

  @SuppressWarnings("unchecked")
  private TestStreamP(List events, Coder outputCoder) {
    traverser =
        traverseIterable(events)
            .map(
                event -> {
                  if (event instanceof SerializableWatermarkEvent) {
                    long ts = ((SerializableWatermarkEvent) event).getTimestamp();
                    if (ts == Long.MAX_VALUE) {
                      // this is an element added by advanceWatermarkToInfinity(), we ignore it,
                      // it's always at the end
                      return null;
                    }
                    return new Watermark(ts);
                  } else {
                    assert event instanceof SerializableTimestampedValue;
                    WindowedValue windowedValue =
                        ((SerializableTimestampedValue) event).asWindowedValue();
                    return Utils.encodeWindowedValue(windowedValue, outputCoder, baos);
                  }
                });
  }

  public static <T> ProcessorMetaSupplier supplier(
      List<TestStream.Event<T>> events, Coder outputCoder) {
    List<Object> serializableEvents = getSerializableEvents(events);
    return ProcessorMetaSupplier.forceTotalParallelismOne(
        ProcessorSupplier.of(() -> new TestStreamP(serializableEvents, outputCoder)));
  }

  private static <T> List<Object> getSerializableEvents(List<TestStream.Event<T>> events) {
    return events.stream()
        .flatMap(
            e -> {
              if (e instanceof TestStream.WatermarkEvent) {
                return Stream.of(
                    new SerializableWatermarkEvent(
                        ((TestStream.WatermarkEvent<T>) e).getWatermark().getMillis()));
              } else if (e instanceof TestStream.ElementEvent) {
                return StreamSupport.stream(
                        ((TestStream.ElementEvent<T>) e).getElements().spliterator(), false)
                    .map(
                        te -> new SerializableTimestampedValue<>(te.getValue(), te.getTimestamp()));
              } else {
                throw new UnsupportedOperationException(
                    "Event type not supported in TestStream: " + e.getClass() + ", event: " + e);
              }
            })
        .collect(toList());
  }

  @Override
  public boolean complete() {
    // todo: TestStream says it should cease emitting, but not stop after the items.
    //   But I don't know how they end the job otherwise...
    return emitFromTraverser(traverser);
  }

  private static class SerializableWatermarkEvent implements Serializable {
    private final long timestamp;

    SerializableWatermarkEvent(long timestamp) {
      this.timestamp = timestamp;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }

  private static class SerializableTimestampedValue<T> implements Serializable {
    private final T value;
    private final Instant timestamp;

    SerializableTimestampedValue(@Nullable T value, Instant timestamp) {
      this.value = value;
      this.timestamp = timestamp;
    }

    WindowedValue<T> asWindowedValue() {
      return WindowedValue.timestampedValueInGlobalWindow(value, timestamp);
    }
  }
}
