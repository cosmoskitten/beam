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
public class MetricFilter {

  private final Set<MetricName> names;
  private final Set<String> scopes;

  private MetricFilter(Set<MetricName> names, Set<String> scopes) {
    this.names = names;
    this.scopes = scopes;
  }

  public Set<MetricName> names() {
    return names;
  }

  public Set<String> scopes() {
    return scopes;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for creating a {@link MetricFilter}.
   */
  public static class Builder {
    private ImmutableSet.Builder<MetricName> namesBuilder = ImmutableSet.builder();
    private ImmutableSet.Builder<String> scopesBuilder = ImmutableSet.builder();

    /**
     * Add a {@link MetricName MetricNames} to the filter. If the {@link MetricName#getName()} is
     * {@code null} this will include all metrics within the given namespace. If no name filters are
     * specified then metrics will be returned regardless of what name they have.
     */
    public Builder addName(MetricName value) {
      namesBuilder.add(value);
      return this;
    }

    /**
     * Add a PTransform-step to the filter. If no steps are specified then metrics will be included
     * regardless of which step they came from.
     */
    public Builder addStep(String step) {
      scopesBuilder.add(step);
      return this;
    }

    public MetricFilter build() {
      return new MetricFilter(namesBuilder.build(), scopesBuilder.build());
    }
  }
}
