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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.transforms.Create.Values;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.AssignWindows;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;

/**
 * An in-process implementation of the {@link Values Create.Values} {@link PTransform}, implemented
 * using a {@link BoundedSource}.
 *
 * The coder is inferred via the {@link Values#getDefaultOutputCoder(PInput)} method on the original
 * transform.
 */
class InProcessWindowInto<T> extends ForwardingPTransform<PCollection<T>, PCollection<T>> {
  private final Window.Bound<T> original;

  public static <T> InProcessWindowInto<T> from(Window.Bound<T> original) {
    return new InProcessWindowInto<>(original);
  }

  private InProcessWindowInto(Window.Bound<T> original) {
    this.original = original;
  }

  @Override
  public PCollection<T> apply(PCollection<T> input) {
      WindowingStrategy<?, ?> outputStrategy =
          original.getOutputStrategyInternal(input.getWindowingStrategy());
      return input.apply(
          new AssignWindows<>(original.getWindowFn())).setWindowingStrategyInternal(outputStrategy);
  }

  @Override
  public PTransform<PCollection<T>, PCollection<T>> delegate() {
    return original;
  }
}
