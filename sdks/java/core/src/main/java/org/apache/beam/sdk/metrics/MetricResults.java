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

import com.google.common.base.Objects;
import java.util.Set;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * Methods for interacting with the metrics of a pipeline that has been executed. Accessed via
 * {@link PipelineResult#metrics()}.
 */
@Experimental(Kind.METRICS)
public abstract class MetricResults {
  /**
   * Query for all metrics that match the filter.
   */
  public abstract MetricQueryResults queryMetrics(MetricsFilter filter);

  // Matching logic is implemented here rather than in MetricsFilter because we would like
  // MetricsFilter to act as a "dumb" value-object, with the possibility of replacing it with
  // a Proto/JSON/etc. schema object.
  protected boolean matches(MetricsFilter filter, MetricKey key) {
    return filter == null
        || (matchesName(key.metricName(), filter.names())
        && matchesScope(key.stepName(), filter.steps()));
  }

  /**
   * {@code subPathMatches(haystack, needle)} returns true if {@code needle}
   * represents a path within {@code haystack}. For example, "foo/bar" is in "a/foo/bar/b",
   * but not "a/fool/bar/b" or "a/foo/bart/b".
   */
  public boolean subPathMatches(String haystack, String needle) {
    int location = haystack.indexOf(needle);
    int end = location + needle.length();
    if (location == -1) {
      return false;  // needle not found
    } else if (location != 0 && haystack.charAt(location - 1) != '/') {
      return false; // the first entry in needle wasn't exactly matched
    } else if (end != haystack.length() && haystack.charAt(end) != '/') {
      return false; // the last entry in needle wasn't exactly matched
    } else {
      return true;
    }
  }

  /**
   * {@code matchesScope(actualScope, scopes)} returns true if the scope of a metric is matched
   * by any of the filters in {@code scopes}. A metric scope is a path of type "A/B/D". A
   * path is matched by a filter if the filter is equal to the path (e.g. "A/B/D", or
   * if it represents a subpath within it (e.g. "A/B" or "B/D", but not "A/D"). */
  public boolean matchesScope(String actualScope, Set<String> scopes) {
    if (scopes.isEmpty() || scopes.contains(actualScope)) {
      return true;
    }

    // If there is no perfect match, a stage name-level match is tried.
    // This is done by a substring search over the levels of the scope.
    // e.g. a scope "A/B/C/D" is matched by "A/B", but not by "A/C".
    for (String scope : scopes) {
      if (subPathMatches(actualScope, scope)) {
        return true;
      }
    }

    return false;
  }

  private boolean matchesName(MetricName metricName, Set<MetricNameFilter> nameFilters) {
    if (nameFilters.isEmpty()) {
      return true;
    }

    for (MetricNameFilter nameFilter : nameFilters) {
      if ((nameFilter.getName() == null || nameFilter.getName().equals(metricName.name()))
          && Objects.equal(metricName.namespace(), nameFilter.getNamespace())) {
        return true;
      }
    }

    return false;
  }
}
