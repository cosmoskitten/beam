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
package org.apache.beam.runners.flink.translation.wrappers.streaming.stableinput;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

/** Elements which can be buffered as part of a checkpoint for @RequiresStableInput. */
class BufferedElements {

  static class Timer implements BufferedElement {

    private final String timerId;
    private final BoundedWindow window;
    private final Instant timestamp;
    private final TimeDomain timeDomain;

    Timer(String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain) {
      this.timerId = timerId;
      this.window = window;
      this.timestamp = timestamp;
      this.timeDomain = timeDomain;
    }

    @Override
    public void processWith(DoFnRunner doFnRunner) {
      doFnRunner.onTimer(timerId, window, timestamp, timeDomain);
    }
  }

  static class Element implements BufferedElement {
    private final WindowedValue element;

    Element(WindowedValue element) {
      this.element = element;
    }

    @Override
    public void processWith(DoFnRunner doFnRunner) {
      doFnRunner.processElement(element);
    }
  }

  static class BufferedElementCoder extends Coder<BufferedElement> {

    private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();
    private static final InstantCoder INSTANT_CODER = InstantCoder.of();

    private final Coder<WindowedValue> elementCoder;
    private final Coder<BoundedWindow> windowCoder;

    public BufferedElementCoder(
        Coder<WindowedValue> elementCoder, Coder<BoundedWindow> windowCoder) {
      this.elementCoder = elementCoder;
      this.windowCoder = windowCoder;
    }

    @Override
    public void encode(BufferedElement value, OutputStream outStream) throws IOException {
      if (value instanceof Element) {
        outStream.write(0);
        elementCoder.encode(((Element) value).element, outStream);
      } else if (value instanceof Timer) {
        outStream.write(3);
        Timer timer = (Timer) value;
        STRING_CODER.encode(timer.timerId, outStream);
        windowCoder.encode(timer.window, outStream);
        INSTANT_CODER.encode(timer.timestamp, outStream);
        outStream.write(timer.timeDomain.ordinal());
      } else {
        throw new IllegalStateException("Unexpected element " + value);
      }
    }

    @Override
    public BufferedElement decode(InputStream inStream) throws IOException {
      int firstByte = inStream.read();
      switch (firstByte) {
        case 0:
          return new Element(elementCoder.decode(inStream));
        case 1:
          return new Timer(
              STRING_CODER.decode(inStream),
              windowCoder.decode(inStream),
              INSTANT_CODER.decode(inStream),
              TimeDomain.values()[inStream.read()]);
        default:
          throw new IllegalStateException(
              "Unexpected byte while reading BufferedElement: " + firstByte);
      }
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() {}
  }
}
