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
package org.apache.beam.sdk.loadtests;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testutils.TestResult;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;

/** POJO that represents load test results. */
public class LoadTestResult implements TestResult {

  private final Long timestamp;

  private final Long runtime;

  private final Long totalBytesCount;

  private LoadTestResult(Long timestamp, Long runtime, Long totalBytesCount) {
    this.timestamp = timestamp;
    this.runtime = runtime;
    this.totalBytesCount = totalBytesCount;
  }

  /** Constructs {@link LoadTestResult} from {@link PipelineResult}. */
  static LoadTestResult create(PipelineResult result, String namespace, long now) {
    MetricsReader reader = new MetricsReader(result, namespace);

    return new LoadTestResult(
        now,
        reader.getEndTimeMetric(now, "runtime") - reader.getStartTimeMetric(now, "runtime"),
        reader.getCounterMetric("totalBytes.count", -1));
  }

  public Long getRuntime() {
    return runtime;
  }

  public Long getTotalBytesCount() {
    return totalBytesCount;
  }

  @Override
  public Map<String, Object> toMap() {
    return ImmutableMap.<String, Object>builder()
        .put("timestamp", timestamp)
        .put("runtime", runtime)
        .put("totalBytesCount", totalBytesCount)
        .build();
  }
}
