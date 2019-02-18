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

import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;

/** Utility for parsing a URN to a {@link org.apache.beam.sdk.metrics.MetricName}. */
public class MetricUrns {
  public static final String ELEMENT_COUNT_URN =
      BeamUrns.getUrn(BeamFnApi.MonitoringInfoUrns.Enum.ELEMENT_COUNT);
  public static final String START_BUNDLE_MSECS_URN =
      BeamUrns.getUrn(BeamFnApi.MonitoringInfoUrns.Enum.START_BUNDLE_MSECS);
  public static final String PROCESS_BUNDLE_MSECS_URN =
      BeamUrns.getUrn(BeamFnApi.MonitoringInfoUrns.Enum.PROCESS_BUNDLE_MSECS);
  public static final String FINISH_BUNDLE_MSECS_URN =
      BeamUrns.getUrn(BeamFnApi.MonitoringInfoUrns.Enum.FINISH_BUNDLE_MSECS);
  public static final String USER_METRIC_URN_PREFIX =
      BeamUrns.getUrn(BeamFnApi.MonitoringInfoUrns.Enum.USER_METRIC_URN_PREFIX);
  public static final String SUM_INT64_TYPE_URN =
      BeamUrns.getUrn(BeamFnApi.MonitoringInfoTypeUrns.Enum.SUM_INT64_TYPE);
  public static final String DISTRIBUTION_INT64_TYPE_URN =
      BeamUrns.getUrn(BeamFnApi.MonitoringInfoTypeUrns.Enum.DISTRIBUTION_INT64_TYPE);
  public static final String LATEST_INT64_TYPE_URN =
      BeamUrns.getUrn(BeamFnApi.MonitoringInfoTypeUrns.Enum.LATEST_INT64_TYPE);
  public static final String PCOLLECTION_LABEL =
      getLabelString(BeamFnApi.MonitoringInfo.MonitoringInfoLabels.PCOLLECTION);
  public static final String PTRANSFORM_LABEL =
      getLabelString(BeamFnApi.MonitoringInfo.MonitoringInfoLabels.PTRANSFORM);

  /**
   * Parse a {@link org.apache.beam.sdk.metrics.MetricName} from a {@link
   * org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoUrns.Enum}.
   *
   * <p>Should be consistent with {@code parse_namespace_and_name} in monitoring_infos.py.
   */
  @Nullable
  public static MetricName parseUrn(String urn) {
    if (urn.startsWith(USER_METRIC_URN_PREFIX)) {
      urn = urn.substring(USER_METRIC_URN_PREFIX.length());
    } else {
      return null;
    }
    // If it is not a user counter, just use the first part of the URN, i.e. 'beam'
    String[] pieces = urn.split(":", 2);
    if (pieces.length != 2) {
      throw new IllegalArgumentException("Invalid metric URN: " + urn);
    }
    return MetricName.named(pieces[0], pieces[1]);
  }

  /** Returns the label string constant defined in the MonitoringInfoLabel enum proto. */
  private static String getLabelString(BeamFnApi.MonitoringInfo.MonitoringInfoLabels label) {
    BeamFnApi.MonitoringInfoLabelProps props =
        label.getValueDescriptor().getOptions().getExtension(BeamFnApi.labelProps);
    return props.getName();
  }
}
