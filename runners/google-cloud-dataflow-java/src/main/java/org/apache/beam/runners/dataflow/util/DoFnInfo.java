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
package org.apache.beam.runners.dataflow.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Wrapper class holding the necessary information to serialize a {@link OldDoFn}
 * or {@link DoFn}.
 *
 * @param <InputT> the type of the (main) input elements of the {@link OldDoFn}
 * @param <OutputT> the type of the (main) output elements of the {@link OldDoFn}
 */
public class DoFnInfo<InputT, OutputT> implements Serializable {
  private final Serializable oldDoFnOrDoFn;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final Iterable<PCollectionView<?>> sideInputViews;
  private final Coder<InputT> inputCoder;
  private final long mainOutput;
  private final Map<Long, TupleTag<?>> outputMap;

  /** Lazily initialized invoker for this {@link DoFn} */
  @Nullable
  private transient DoFnInvoker<InputT, OutputT> doFnInvoker;

  /** Creates a {@link DoFnInfo} for the given {@link DoFn} and auxiliary bits and pieces. */
  public DoFnInfo(
      DoFn<InputT, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Iterable<PCollectionView<?>> sideInputViews,
      Coder<InputT> inputCoder,
      long mainOutput,
      Map<Long, TupleTag<?>> outputMap) {
    this((Serializable) doFn, windowingStrategy, sideInputViews, inputCoder, mainOutput, outputMap);
  }

  /** Uses of this constructor should be ported to pass a {@link DoFn}. */
  @Deprecated
  public DoFnInfo(
      OldDoFn<InputT, OutputT> oldDoFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Iterable<PCollectionView<?>> sideInputViews,
      Coder<InputT> inputCoder,
      long mainOutput,
      Map<Long, TupleTag<?>> outputMap) {
    this((Serializable) oldDoFn, windowingStrategy, sideInputViews, inputCoder, mainOutput, outputMap);
  }

  /** This constructor provides a migration path from {@link OldDoFn} to {@link DoFn}. */
  @Deprecated
  public DoFnInfo(
      Serializable oldDoFnOrDoFn,
      WindowingStrategy<?, ?> windowingStrategy,
      Iterable<PCollectionView<?>> sideInputViews,
      Coder<InputT> inputCoder,
      long mainOutput,
      Map<Long, TupleTag<?>> outputMap) {
    checkArgument(
        oldDoFnOrDoFn instanceof DoFn || oldDoFnOrDoFn instanceof OldDoFn,
        "%s fn argument must be a %s or an %s",
        getClass().getSimpleName(),
        DoFn.class.getSimpleName(),
        OldDoFn.class.getSimpleName());
    this.oldDoFnOrDoFn = oldDoFnOrDoFn;
    this.windowingStrategy = windowingStrategy;
    this.sideInputViews = sideInputViews;
    this.inputCoder = inputCoder;
    this.mainOutput = mainOutput;
    this.outputMap = outputMap;
  }

  /**
   * @deprecated callers should use {@link #getFn()} to be agnostic as to whether
   * this contains a {@link DoFn} or an {@link OldDoFn} or, eventually, a serialized
   * blob from another language.
   */
  @Deprecated
  public OldDoFn<InputT, OutputT> getDoFn() {
    checkState(oldDoFnOrDoFn instanceof OldDoFn,
        "%s.getDoFn() called when it did not contain %s",
        DoFnInfo.class.getSimpleName(), OldDoFn.class.getSimpleName());
    return (OldDoFn) oldDoFnOrDoFn;
  }

  /**
   * Returns the embedded serialized function. It may be a {@code DoFn} or {@code OldDoFn}.
   */
  public Serializable getFn() {
    return oldDoFnOrDoFn;
  }

  public WindowingStrategy<?, ?> getWindowingStrategy() {
    return windowingStrategy;
  }

  public Iterable<PCollectionView<?>> getSideInputViews() {
    return sideInputViews;
  }

  public Coder<InputT> getInputCoder() {
    return inputCoder;
  }

  public long getMainOutput() {
    return mainOutput;
  }

  public Map<Long, TupleTag<?>> getOutputMap() {
    return outputMap;
  }
}
