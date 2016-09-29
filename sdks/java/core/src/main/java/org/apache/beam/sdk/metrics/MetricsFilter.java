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

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * Simple POJO representing a filter for querying metrics.
 */
@Experimental(Kind.METRICS)
public class MetricsFilter {

  private final Set<MetricNameFilter> names;
  private final Set<String> steps;

  private MetricsFilter(Set<MetricNameFilter> names, Set<String> steps) {
    this.names = names;
    this.steps = steps;
  }

  public Set<MetricNameFilter> names() {
    return names;
  }

  public Set<String> steps() {
    return steps;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for creating a {@link MetricsFilter}.
   */
  public static class Builder {
    private ImmutableSet.Builder<MetricNameFilter> namesBuilder = ImmutableSet.builder();
    private ImmutableSet.Builder<String> stepsBuilder = ImmutableSet.builder();

    /**
     * Add a {@link MetricNameFilter}.
     *
     * <p>If no name filters are specified then metrics will be returned regardless of what name
     * they have.
     */
    public Builder addNameFilter(MetricNameFilter nameFilter) {
      namesBuilder.add(nameFilter);
      return this;
    }

    /**
     * Add a step filter.
     *
     * <p>If no steps are specified then metrics will be included regardless of which step
     * came from.
     */
    public Builder addStep(String step) {
      stepsBuilder.add(step);
      return this;
    }

    public MetricsFilter build() {
      return new MetricsFilter(namesBuilder.build(), stepsBuilder.build());
    }
  }
}
