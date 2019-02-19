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
package org.apache.beam.runners.core.metrics;

import static org.apache.beam.model.fnexecution.v1.BeamFnApi.IntDistributionData;
import static org.apache.beam.model.fnexecution.v1.BeamFnApi.IntGaugeData;
import static org.apache.beam.sdk.metrics.MetricUrns.DISTRIBUTION_INT64_TYPE_URN;
import static org.apache.beam.sdk.metrics.MetricUrns.ELEMENT_COUNT_URN;
import static org.apache.beam.sdk.metrics.MetricUrns.LATEST_INT64_TYPE_URN;
import static org.apache.beam.sdk.metrics.MetricUrns.PCOLLECTION_LABEL;
import static org.apache.beam.sdk.metrics.MetricUrns.PTRANSFORM_LABEL;
import static org.apache.beam.sdk.metrics.MetricUrns.SUM_INT64_TYPE_URN;
import static org.apache.beam.sdk.metrics.MetricUrns.urn;

import java.time.Instant;
import java.util.HashMap;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoSpec;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoSpecs;
import org.apache.beam.sdk.metrics.DistributionProtos;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeProtos;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simplified building of MonitoringInfo fields, allows setting one field at a time with simpler
 * method calls, without needing to dive into the details of the nested protos.
 *
 * <p>There is no need to set the type field, by setting the appropriate value field: (i.e.
 * setInt64Value), the typeUrn field is automatically set.
 *
 * <p>Additionally, if validateAndDropInvalid is set to true in the ctor, then MonitoringInfos will
 * be returned as null when build() is called if any fields are not properly set. This is based on
 * comparing the fields which are set to the MonitoringInfoSpec in beam_fn_api.proto.
 *
 * <p>Example Usage (ElementCount counter):
 *
 * <p>SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
 * builder.setUrn(SimpleMonitoringInfoBuilder.ELEMENT_COUNT_URN); builder.setInt64Value(1);
 * builder.setPTransformLabel("myTransform"); builder.setPCollectionLabel("myPcollection");
 * MonitoringInfo mi = builder.build();
 *
 * <p>Example Usage (ElementCount counter):
 *
 * <p>SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
 * builder.setUrn(SimpleMonitoringInfoBuilder.setUrnForUserMetric("myNamespace", "myName"));
 * builder.setInt64Value(1); MonitoringInfo mi = builder.build();
 */
public class SimpleMonitoringInfoBuilder {

  private static final HashMap<String, MonitoringInfoSpec> specs =
      new HashMap<String, MonitoringInfoSpec>();

  private final boolean validateAndDropInvalid;

  private static final Logger LOG = LoggerFactory.getLogger(SimpleMonitoringInfoBuilder.class);

  private MonitoringInfo.Builder builder;

  private SpecMonitoringInfoValidator validator = new SpecMonitoringInfoValidator();

  static {
    for (MonitoringInfoSpecs.Enum val : MonitoringInfoSpecs.Enum.values()) {
      // The enum iterator inserts an UNRECOGNIZED = -1 value which isn't explicitly added in
      // the proto files.
      if (!val.name().equals("UNRECOGNIZED")) {
        MonitoringInfoSpec spec =
            val.getValueDescriptor().getOptions().getExtension(BeamFnApi.monitoringInfoSpec);
        SimpleMonitoringInfoBuilder.specs.put(spec.getUrn(), spec);
      }
    }
  }

  public SimpleMonitoringInfoBuilder() {
    this(true);
  }

  public SimpleMonitoringInfoBuilder(boolean validateAndDropInvalid) {
    this.builder = MonitoringInfo.newBuilder();
    this.validateAndDropInvalid = validateAndDropInvalid;
  }

  /**
   * Sets the urn of the MonitoringInfo.
   *
   * @param urn The urn of the MonitoringInfo
   */
  public SimpleMonitoringInfoBuilder setUrn(String urn) {
    this.builder.setUrn(urn);
    return this;
  }

  public SimpleMonitoringInfoBuilder handleMetricKey(MetricKey key) {
    builder.setUrn(key.metricName().urn()).putAllLabels(key.labels().map());
    return this;
  }

  public SimpleMonitoringInfoBuilder userMetric(String ptransform, String namespace, String name) {
    return setUrn(urn(namespace, name)).setPTransformLabel(ptransform);
  }

  /**
   * Sets the urn of the MonitoringInfo to a proper user metric URN for the given params.
   *
   * @param namespace
   * @param name
   */
  public SimpleMonitoringInfoBuilder setUrnForUserMetric(String namespace, String name) {
    this.builder.setUrn(urn(namespace, name));
    return this;
  }

  /** Sets the timestamp of the MonitoringInfo to the current time. */
  public SimpleMonitoringInfoBuilder setTimestampToNow() {
    Instant time = Instant.now();
    this.builder.getTimestampBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano());
    return this;
  }

  /** Sets the int64Value of the CounterData in the MonitoringInfo, and the appropriate type URN. */
  public SimpleMonitoringInfoBuilder setInt64Value(long value) {
    this.builder.getMetricBuilder().setCounter(value);
    this.setInt64TypeUrn();
    return this;
  }

  /** Sets the the appropriate type URN for sum int64 counters. */
  public SimpleMonitoringInfoBuilder setInt64TypeUrn() {
    this.builder.setType(SUM_INT64_TYPE_URN);
    return this;
  }

  public SimpleMonitoringInfoBuilder setIntDistributionValue(DistributionData value) {
    return setIntDistributionValue(value.extractResult());
  }

  public SimpleMonitoringInfoBuilder setIntDistributionValue(DistributionResult value) {
    return setIntDistributionValue(DistributionProtos.toProto(value));
  }

  /** Sets the int64Value of the CounterData in the MonitoringInfo, and the appropraite type URN. */
  public SimpleMonitoringInfoBuilder setIntDistributionValue(IntDistributionData value) {
    this.builder.getMetricBuilder().setDistribution(value);
    this.builder.setType(DISTRIBUTION_INT64_TYPE_URN);
    return this;
  }

  public SimpleMonitoringInfoBuilder setGaugeValue(GaugeData value) {
    return setGaugeValue(value.extractResult());
  }

  public SimpleMonitoringInfoBuilder setGaugeValue(GaugeResult value) {
    return setGaugeValue(GaugeProtos.toProto(value));
  }

  /** Sets the int64Value of the CounterData in the MonitoringInfo, and the appropraite type URN. */
  public SimpleMonitoringInfoBuilder setGaugeValue(IntGaugeData value) {
    this.builder.getMetricBuilder().setGauge(value);
    this.builder.setType(LATEST_INT64_TYPE_URN);
    return this;
  }

  /** Sets the PTRANSFORM MonitoringInfo label to the given param. */
  public SimpleMonitoringInfoBuilder setPTransformLabel(String pTransform) {
    // TODO(ajamato): Add validation that it is a valid pTransform name in the bundle descriptor.
    setLabel(PTRANSFORM_LABEL, pTransform);
    return this;
  }

  public SimpleMonitoringInfoBuilder forElementCount(String pCollection) {
    return setLabel(PCOLLECTION_LABEL, pCollection).setUrn(ELEMENT_COUNT_URN);
  }

  /** Sets the PCOLLECTION MonitoringInfo label to the given param. */
  public SimpleMonitoringInfoBuilder setPCollectionLabel(String pCollection) {
    setLabel(PCOLLECTION_LABEL, pCollection);
    return this;
  }

  /** Sets the MonitoringInfo label to the given name and value. */
  public SimpleMonitoringInfoBuilder setLabel(String labelName, String labelValue) {
    this.builder.putLabels(labelName, labelValue);
    return this;
  }

  /** Clear the builder and merge from the provided monitoringInfo. */
  public void clearAndMerge(MonitoringInfo monitoringInfo) {
    this.builder = MonitoringInfo.newBuilder();
    this.builder.mergeFrom(monitoringInfo);
  }

  /**
   * @return A copy of the MonitoringInfo with the timestamp cleared, to allow comparing two
   *     MonitoringInfos.
   */
  @VisibleForTesting
  public static MonitoringInfo clearTimestamp(MonitoringInfo input) {
    MonitoringInfo.Builder builder = MonitoringInfo.newBuilder();
    builder.mergeFrom(input);
    builder.clearTimestamp();
    return builder.build();
  }

  /**
   * Builds the provided MonitoringInfo. Returns null if validateAndDropInvalid set and fields do
   * not match respecting MonitoringInfoSpec based on urn.
   */
  @Nullable
  public MonitoringInfo build() {
    final MonitoringInfo result = this.builder.build();
    if (validateAndDropInvalid) {
      Optional<String> error = this.validator.validate(result);
      if (error.isPresent()) {
        // TODO(ryan): throw in this case; remove nullability
        LOG.warn("Dropping invalid partial MonitoringInfo: {}\n{}", error, result);
        return null;
      }
    }
    return result;
  }
}
