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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * A wrapper translating Flink sinks implementing the {@link SinkFunction} interface, into
 * Beam {@link PTransform}).
 * */
public class UnboundedFlinkSink<T> extends PTransform<PCollection<T>, PDone> {

  /* The Flink sink function */
  private final SinkFunction<T> flinkSink;

  private UnboundedFlinkSink(SinkFunction<T> flinkSink) {
    this.flinkSink = flinkSink;
  }

  public SinkFunction<T> getFlinkSource() {
    return this.flinkSink;
  }

  @Override
  public PDone expand(PCollection<T> input) {
    // The Flink translator injects the write to the {@link SinkFunction}
    return PDone.in(input.getPipeline());
  }


  /**
   * Creates a Flink sink to write to .
   * @param flinkSink The Flink sink, e.g. FlinkKafkaProducer09
   * @param <T> The input type of the sink
   * @return A {@link PTransform} wrapping a Flink sink
   */
  public static <T> UnboundedFlinkSink<T> of(SinkFunction<T> flinkSink) {
    return new UnboundedFlinkSink<>(flinkSink);
  }

}
