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
package org.apache.beam.sdk.metrics;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metrics containers by step.
 *
 * <p>This class is not thread-safe.</p>
 */
public class MetricsContainerStepMap implements Serializable {
  private Map<String, MetricsContainer> metricsContainers;

  public MetricsContainerStepMap() {
    this.metricsContainers = new ConcurrentHashMap<>();
  }

  /**
   * Returns the container for the given step name.
   */
  public MetricsContainer getContainer(String stepName) {
    if (!metricsContainers.containsKey(stepName)) {
      metricsContainers.put(stepName, new MetricsContainer(stepName));
    }
    return metricsContainers.get(stepName);
  }

  /**
   * Update this {@link MetricsContainerStepMap} with all values from given
   * {@link MetricsContainerStepMap}.
   */
  public void updateAll(MetricsContainerStepMap other) {
    for (Map.Entry<String, MetricsContainer> container : other.metricsContainers.entrySet()) {
      getContainer(container.getKey()).update(container.getValue());
    }
  }

  /**
   * Update {@link MetricsContainer} for given step in this map with all values from given
   * {@link MetricsContainer}.
   */
  public void update(String step, MetricsContainer container) {
    getContainer(step).update(container);
  }

  /**
   * Returns a map from step name to {@link MetricsContainer} representation of this
   * {@link MetricsContainerStepMap}.
   */
  public Map<String, MetricsContainer> asMap() {
    return new HashMap<>(metricsContainers);
  }
}
