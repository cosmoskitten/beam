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

package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.UUID;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;

/** Tags each value with a unique id. */
@VisibleForTesting
class TagWithUniqueIds<K, V> extends DoFn<KV<K, V>, KV<K, KV<String, V>>> {
  private transient String randomUUID;
  private transient long sequenceNo = 0L;

  @Setup
  public void setup() {
    randomUUID = UUID.randomUUID().toString();
  }

  /** Tag the input with a unique id. */
  @ProcessElement
  public void processElement(ProcessContext context, BoundedWindow window) throws IOException {
    context.output(
        KV.of(
            context.element().getKey(),
            KV.of(randomUUID + sequenceNo++, context.element().getValue())));
  }
}
