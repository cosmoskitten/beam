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

package org.apache.beam.runners.spark.metrics;

import java.io.Serializable;
import org.apache.beam.sdk.metrics.DistributionData;
import org.apache.beam.sdk.metrics.MetricKey;


/**
 * Metric values wrapper which adds aggregation methods.
 * @param <ValueT> Metric value type.
 */
abstract class MetricAggregator<ValueT> implements Serializable {
  private final MetricKey key;
  protected ValueT value;

  private MetricAggregator(MetricKey key, ValueT value) {
    this.key = key;
    this.value = value;
  }

  public MetricKey getKey() {
    return key;
  }

  public ValueT getValue() {
    return value;
  }

  @SuppressWarnings("unused")
  abstract MetricAggregator<ValueT> updated(ValueT t);

  static class CounterAggregator extends MetricAggregator<Long> {
    CounterAggregator(MetricKey key, Long value) {
      super(key, value);
    }

    @Override
    CounterAggregator updated(Long counterUpdate) {
      value = value + counterUpdate;
      return this;
    }
  }

  static class DistributionAggregator extends MetricAggregator<DistributionData> {
    DistributionAggregator(MetricKey key, DistributionData value) {
      super(key, value);
    }

    @Override
    DistributionAggregator updated(DistributionData distributionUpdate) {
      value = value.combine(distributionUpdate);
      return this;
    }
  }
}

