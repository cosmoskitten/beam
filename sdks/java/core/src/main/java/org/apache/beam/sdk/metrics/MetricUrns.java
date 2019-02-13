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

import static org.apache.beam.sdk.metrics.SimpleMonitoringInfoBuilder.USER_COUNTER_URN_PREFIX;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Strings;

/** Utility for parsing a URN to a {@link org.apache.beam.sdk.metrics.MetricName}. */
public class MetricUrns {
  /**
   * Parse a {@link org.apache.beam.sdk.metrics.MetricName} from a {@link
   * org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoUrns.Enum}.
   *
   * <p>Should be consistent with {@code parse_namespace_and_name} in monitoring_infos.py.
   */
  @Nullable
  public static MetricName parseUrn(String urn) {
    if (urn.startsWith(USER_COUNTER_URN_PREFIX)) {
      urn = urn.substring(USER_COUNTER_URN_PREFIX.length());
    } else {
      return null;
    }
    List<String> pieces = Splitter.on(':').splitToList(urn);
    if (pieces.size() != 2) {
      throw new IllegalArgumentException(
          "Invalid metric URN: " + urn + ". Expected two ':'-delimited segments (namespace, name)");
    }
    return MetricName.named(pieces.get(0), pieces.get(1));
  }

  //  public static Collection<String> urnPieces(String urn) {
  //    List<String> pieces = Splitter.on(':').splitToList(urn);
  //    int idx = 0;
  //    if (pieces.get(idx).equals("beam")) {
  //      idx += 1;
  //      if (pieces.get(idx).equals("metric")) {
  //        idx += 1;
  //      }
  //    }
  //
  //    return pieces.subList(idx, pieces.size());
  //  }

  public static String urn(String namespace, String name) {
    checkArgument(namespace != null, "Metric namespace must be non-null");
    checkArgument(!Strings.isNullOrEmpty(name), "Metric name must be non-empty");
    return String.join(":", USER_COUNTER_URN_PREFIX, namespace, name);
  }
}
