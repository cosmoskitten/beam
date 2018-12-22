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
package org.apache.beam.runners.samza.runtime;

import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.samza.task.TaskContext;

/** A registrar for Samza DoFnInvoker. */
public interface SamzaDoFnInvokerRegistrar {

  /** Returns the invoker for a {@link DoFn}. */
  <InputT, OutputT> DoFnInvoker<InputT, OutputT> invokerFor(
      DoFn<InputT, OutputT> fn, TaskContext context);

  /** Returns the configs for a {@link DoFn}. */
  <InputT, OutputT> Map<String, String> configFor(DoFn<InputT, OutputT> fn);
}
