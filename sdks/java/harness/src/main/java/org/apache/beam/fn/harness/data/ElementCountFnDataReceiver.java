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
package org.apache.beam.fn.harness.data;

import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * A wrapping FnDataReceiverWindowedValue<T> which counts the number of elements consumed by the
 * original nDataReceiverWindowedValue<T>.
 *
 * @param <T> - The receiving type of the PTransform.
 */
public class ElementCountFnDataReceiver<T> implements FnDataReceiver<WindowedValue<T>> {

  private FnDataReceiver<WindowedValue<T>> original;
  private String pCollection;

  public ElementCountFnDataReceiver(FnDataReceiver<WindowedValue<T>> original, String pCollection) {
    this.original = original;
    this.pCollection = pCollection;
  }

  @Override
  public void accept(WindowedValue<T> input) throws Exception {
    String name = "ElementCount";
    String namespace = SimpleMonitoringInfoBuilder.ELEMENT_COUNT_URN;
    Metrics.counter(namespace, name).inc(input.getWindows().size());
    this.original.accept(input);
  }
}
