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
package org.apache.beam.runners.dataflow.worker;

import static com.google.api.client.util.Base64.decodeBase64;
import static org.apache.beam.runners.dataflow.util.Structs.getString;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.service.AutoService;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;

/** Creates a PartitioningShuffleReader from a CloudObject spec. */
public class PartitioningShuffleReaderFactory implements ReaderFactory {

  /** A {@link ReaderFactory.Registrar} for partitioning shuffle sources. */
  @AutoService(ReaderFactory.Registrar.class)
  public static class Registrar implements ReaderFactory.Registrar {

    @Override
    public Map<String, ReaderFactory> factories() {
      return ImmutableMap.of("PartitioningShuffleSource", new PartitioningShuffleReaderFactory());
    }
  }

  @Override
  public NativeReader<?> create(
      CloudObject spec,
      @Nullable Coder<?> coder,
      @Nullable PipelineOptions options,
      @Nullable DataflowExecutionContext executionContext,
      DataflowOperationContext operationContext)
      throws Exception {
    checkNotNull(options, "PipelineOptions must not be null in PartitioningShuffleReaderFactory");
    @SuppressWarnings({"unchecked", "rawtypes"})
    Coder<WindowedValue<KV<Object, Object>>> typedCoder =
        (Coder<WindowedValue<KV<Object, Object>>>) coder;
    return createTyped(spec, options, typedCoder, executionContext, operationContext);
  }

  public <K, V> PartitioningShuffleReader<K, V> createTyped(
      CloudObject spec,
      PipelineOptions options,
      Coder<WindowedValue<KV<K, V>>> coder,
      @Nullable DataflowExecutionContext executionContext,
      DataflowOperationContext operationContext)
      throws Exception {
    return new PartitioningShuffleReader<K, V>(
        options,
        decodeBase64(getString(spec, WorkerPropertyNames.SHUFFLE_READER_CONFIG)),
        getString(spec, WorkerPropertyNames.START_SHUFFLE_POSITION, null),
        getString(spec, WorkerPropertyNames.END_SHUFFLE_POSITION, null),
        coder,
        (BatchModeExecutionContext) executionContext,
        operationContext);
  }
}
