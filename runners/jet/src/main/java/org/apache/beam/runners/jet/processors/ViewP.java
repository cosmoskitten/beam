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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.SupplierEx;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.beam.runners.jet.Utils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * Jet {@link com.hazelcast.jet.core.Processor} implementation for Beam's side input producing
 * primitives. Collects all input {@link WindowedValue}s, groups them by windows and keys and when
 * input is complete emits them.
 */
public class ViewP extends AbstractProcessor {

  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final TimestampCombiner timestampCombiner;
  private final Coder inputCoder;
  private final Coder outputCoder;

  @SuppressWarnings({"FieldCanBeLocal", "unused"})
  private final String ownerId; // do not remove, useful for debugging

  private Map<BoundedWindow, TimestampAndValues> values = new HashMap<>();
  private PaneInfo paneInfo = PaneInfo.NO_FIRING;
  private Traverser<byte[]> resultTraverser;

  private ViewP(
      Coder inputCoder, Coder outputCoder, WindowingStrategy windowingStrategy, String ownerId) {
    this.timestampCombiner = windowingStrategy.getTimestampCombiner();
    this.inputCoder = inputCoder;
    this.outputCoder =
        Utils.deriveIterableValueCoder((WindowedValue.FullWindowedValueCoder) outputCoder);
    this.ownerId = ownerId;
    // System.out.println(ViewP.class.getSimpleName() + " CREATE ownerId = " + ownerId); //useful
    // for debugging
  }

  public static SupplierEx<Processor> supplier(
      Coder inputCoder,
      Coder outputCoder,
      WindowingStrategy<?, ?> windowingStrategy,
      String ownerId) {
    return () -> new ViewP(inputCoder, outputCoder, windowingStrategy, ownerId);
  }

  @Override
  protected boolean tryProcess(int ordinal, @Nonnull Object item) {
    // System.out.println(ViewP.class.getSimpleName() + " UPDATE ownerId = " + ownerId + ", item = "
    // + item); //useful for debugging
    WindowedValue<?> windowedValue = Utils.decodeWindowedValue((byte[]) item, inputCoder);
    for (BoundedWindow window : windowedValue.getWindows()) {
      values.merge(
          window,
          new TimestampAndValues(windowedValue.getTimestamp(), windowedValue.getValue()),
          (o, n) -> o.merge(timestampCombiner, n));
    }

    if (!paneInfo.equals(windowedValue.getPane())) {
      throw new RuntimeException("Oops!");
    }
    return true;
  }

  @Override
  public boolean complete() {
    // System.out.println(ViewP.class.getSimpleName() + " COMPLETE ownerId = " + ownerId); //useful
    // for debugging
    if (resultTraverser == null) {
      resultTraverser =
          Traversers.traverseStream(
              values.entrySet().stream()
                  .map(
                      e -> {
                        WindowedValue<?> outputValue =
                            WindowedValue.of(
                                e.getValue().values,
                                e.getValue().timestamp,
                                Collections.singleton(e.getKey()),
                                paneInfo);
                        return Utils.encodeWindowedValue(outputValue, outputCoder, baos);
                      }));
    }
    return emitFromTraverser(resultTraverser);
  }

  private static class TimestampAndValues {
    private final List<Object> values = new ArrayList<>();
    private Instant timestamp;

    TimestampAndValues(Instant timestamp, Object value) {
      this.timestamp = timestamp;
      values.add(value);
    }

    public Iterable<Object> getValues() {
      return values;
    }

    TimestampAndValues merge(TimestampCombiner timestampCombiner, TimestampAndValues v2) {
      timestamp = timestampCombiner.combine(timestamp, v2.timestamp);
      values.addAll(v2.values);
      return this;
    }
  }
}
