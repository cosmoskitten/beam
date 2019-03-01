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
package org.apache.beam.runners.dataflow.worker.fn.control;

import com.google.api.services.dataflow.model.CounterMetadata;
import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.NameAndKind;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.SpecMonitoringInfoValidator;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MonitoringInfo to CounterUpdate transformer capable to transform MSec counters.
 *
 * <p>Use getSupportedUrns to get all urns this class supports.
 */
public class ElementCountMonitoringInfoToCounterUpdateTransformer
    implements MonitoringInfoToCounterUpdateTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnMapTaskExecutor.class);

  private final SpecMonitoringInfoValidator specValidator;
  private final Map<String, String> sdkToDfePCollectionMapping;
  private static final String supportedUrn = "beam:metric:element_count:v1";

  /**
   * @param specValidator SpecMonitoringInfoValidator to utilize for default validation.
   * @param sdkToDfePCollectionMapping Mapping of SDK available PCollection name to DFE pcollection
   * name.
   */
  public ElementCountMonitoringInfoToCounterUpdateTransformer(
      SpecMonitoringInfoValidator specValidator,
      Map<String, String> sdkToDfePCollectionMapping) {
    this.specValidator = specValidator;
    this.sdkToDfePCollectionMapping = sdkToDfePCollectionMapping;
  }

  /** Allows to inject members for cleaner testing. */
  @VisibleForTesting
  protected ElementCountMonitoringInfoToCounterUpdateTransformer(
      SpecMonitoringInfoValidator specValidator,
      Map<String, String> sdkToDfePCollectionMapping,
      Map<String, String> urnToCounterNameMapping) {
    this.specValidator = specValidator;
    this.sdkToDfePCollectionMapping = sdkToDfePCollectionMapping;
  }

  @VisibleForTesting
  protected Map<String, String> createKnownUrnToCounterNameMapping() {
    Map<String, String> result = new HashMap<>();
    result.put("beam:metric:element_count:v1", "-element-count");
    return result;
  }

  /**
   * Validates provided monitoring info against specs and common safety checks.
   *
   * @param monitoringInfo to validate.
   * @return Optional.empty() all validation checks are passed. Optional with error text otherwise.
   * @throws RuntimeException if received unexpected urn.
   */
  protected Optional<String> validate(MonitoringInfo monitoringInfo) {
    Optional<String> validatorResult = specValidator.validate(monitoringInfo);
    if (validatorResult.isPresent()) {
      return validatorResult;
    }

    String urn = monitoringInfo.getUrn();
    if (!urnToCounterNameMapping.keySet().contains(urn)) {
      throw new RuntimeException(String.format("Received unexpected counter urn: %s", urn));
    }

    //todomigryz: extract and utilize pcollection label from beam_fn_api.proto
    if(!sdkToDfePCollectionMapping.containsKey(monitoringInfo.getLabelsMap("PCOLLECTION"))){
      return Optional.of(
          "Encountered ElementCount MonitoringInfo with unknown PCollectionId: "
              + monitoringInfo.toString());
    }

    return Optional.empty();
  }

  @Override
  public CounterUpdate transform(MonitoringInfo monitoringInfo) {
    Optional<String> validationResult = validate(monitoringInfo);
    if (validationResult.isPresent()) {
      LOG.info(validationResult.get());
      return null;
    }

    long value = monitoringInfo.getMetric().getCounterData().getInt64Value();

    final String pcollectionId = monitoringInfo.getLabelsMap().get("PCOLLECTION");
    final String pcollectionName = sdkToDfePCollectionMapping.get(pcollectionId);

    String counterName = pcollectionName + "-ElementCount";
    NameAndKind name = new NameAndKind();
    name.setName(counterName).setKind("SUM");

    return new CounterUpdate()
        .setNameAndKind(name)
        .setCumulative(true)
        .setInteger(DataflowCounterUpdateExtractor.longToSplitInt(value));
  }

  /** @return iterable of Urns that this transformer can convert to CounterUpdates. */
  public static String getSupportedUrn() {
    return supportedUrn;
  }
}
