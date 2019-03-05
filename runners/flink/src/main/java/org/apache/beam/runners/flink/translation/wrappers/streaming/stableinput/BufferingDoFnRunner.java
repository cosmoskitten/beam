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

import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.joda.time.Instant;

/**
 * A {@link DoFnRunner} which buffers data for supporting {@link
 * org.apache.beam.sdk.transforms.DoFn.RequiresStableInput}.
 *
 * <p>When a DoFn is annotated with @RequiresStableInput we are only allowed to process elements
 * after a checkpoint has completed. This ensures that the input is stable and we produce idempotent
 * results on failures.
 */
public class BufferingDoFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {

  public static <InputT, OutputT> BufferingDoFnRunner<InputT, OutputT> create(
      DoFnRunner<InputT, OutputT> doFnRunner,
      String stateName,
      org.apache.beam.sdk.coders.Coder windowedInputCoder,
      org.apache.beam.sdk.coders.Coder windowCoder,
      OperatorStateBackend operatorStateBackend,
      @Nullable KeyedStateBackend<Object> keyedStateBackend)
      throws Exception {
    return new BufferingDoFnRunner(
        doFnRunner,
        stateName,
        windowedInputCoder,
        windowCoder,
        operatorStateBackend,
        keyedStateBackend);
  }

  private final DoFnRunner<InputT, OutputT> underlying;
  private final BufferingElementsHandler bufferingElementsHandler;

  private BufferingDoFnRunner(
      DoFnRunner<InputT, OutputT> underlying,
      String stateName,
      org.apache.beam.sdk.coders.Coder inputCoder,
      org.apache.beam.sdk.coders.Coder windowCoder,
      OperatorStateBackend operatorStateBackend,
      @Nullable KeyedStateBackend keyedStateBackend)
      throws Exception {
    this.underlying = underlying;
    ListStateDescriptor<BufferedElement> stateDescriptor =
        new ListStateDescriptor<>(
            stateName,
            new CoderTypeSerializer<>(new BufferedElements.Coder(inputCoder, windowCoder)));
    if (keyedStateBackend != null) {
      this.bufferingElementsHandler =
          KeyedBufferingElementsHandler.create(keyedStateBackend, stateDescriptor);
    } else {
      this.bufferingElementsHandler =
          NonKeyedBufferingElementsHandler.create(
              operatorStateBackend.getListState(stateDescriptor));
    }
  }

  @Override
  public void startBundle() {
    // Do not start a bundle, start it later when emitting elements
  }

  @Override
  public void processElement(WindowedValue<InputT> elem) {
    bufferingElementsHandler.buffer(new BufferedElements.Element(elem));
  }

  @Override
  public void onTimer(
      String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain) {
    bufferingElementsHandler.buffer(
        new BufferedElements.Timer(timerId, window, timestamp, timeDomain));
  }

  @Override
  public void finishBundle() {
    // Do not finish a bundle, finish it later when emitting elements
  }

  @Override
  public DoFn<InputT, OutputT> getFn() {
    return underlying.getFn();
  }

  public void emit() {
    Iterator<BufferedElement> iterator = bufferingElementsHandler.getElements().iterator();
    boolean hasElements = iterator.hasNext();
    if (hasElements) {
      underlying.startBundle();
    }
    while (iterator.hasNext()) {
      BufferedElement bufferedElement = iterator.next();
      bufferedElement.processWith(underlying);
    }
    if (hasElements) {
      underlying.finishBundle();
    }
  }
}
