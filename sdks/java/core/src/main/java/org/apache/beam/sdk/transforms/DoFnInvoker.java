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
package org.apache.beam.sdk.transforms;

/** Interface for invoking the {@code OldDoFn} processing methods. */
public interface DoFnInvoker<InputT, OutputT> {
  /** Invoke {@link OldDoFn#startBundle} on the bound {@code OldDoFn}. */
  void invokeStartBundle(DoFn<InputT, OutputT>.Context c);
  /** Invoke {@link OldDoFn#finishBundle} on the bound {@code OldDoFn}. */
  void invokeFinishBundle(DoFn<InputT, OutputT>.Context c);

  /** Invoke {@link OldDoFn#processElement} on the bound {@code OldDoFn}. */
  void invokeProcessElement(
      DoFn<InputT, OutputT>.ProcessContext c, DoFn.ExtraContextFactory<InputT, OutputT> extra);
}
