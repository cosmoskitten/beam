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

import static org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder.PTRANSFORM_LABEL;
import static org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder.USER_COUNTER_URN_PREFIX;

import com.google.api.services.dataflow.model.CounterMetadata;
import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.SpecMonitoringInfoValidator;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.MetricsToCounterUpdateConverter.Origin;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for transforming MonitoringInfo's containing User counter values, to relevant CounterUpdate
 * proto.
 */
class UserMonitoringInfoToCounterUpdateTransformer
    implements MonitoringInfoToCounterUpdateTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnMapTaskExecutor.class);

  private final Map<String, DataflowStepContext> transformIdMapping;

  private final SpecMonitoringInfoValidator specValidator;

  public UserMonitoringInfoToCounterUpdateTransformer(
      final SpecMonitoringInfoValidator specMonitoringInfoValidator,
      final Map<String, DataflowStepContext> transformIdMapping) {
    this.transformIdMapping = transformIdMapping;
    this.specValidator = specMonitoringInfoValidator;
  }

  private Optional<String> validate(MonitoringInfo monitoringInfo) {
    Optional<String> validatorResult = specValidator.validate(monitoringInfo);
    if (validatorResult.isPresent()) {
      return validatorResult;
    }

    String urn = monitoringInfo.getUrn();
    if (!urn.startsWith(USER_COUNTER_URN_PREFIX)) {
      throw new RuntimeException(
          String.format(
              "Received unexpected counter urn. Expected urn starting with: %s, received: %s",
              USER_COUNTER_URN_PREFIX, urn));
    }

    final String ptransform = monitoringInfo.getLabelsMap().get("PTRANSFORM");
    DataflowStepContext stepContext = transformIdMapping.get(ptransform);
    if (stepContext == null) {
      return Optional.of(
          "Encountered user-counter MonitoringInfo with unknown ptransformId: "
              + monitoringInfo.toString());
    }
    return Optional.empty();
  }

  /**
   * Transforms user counter MonitoringInfo to relevant CounterUpdate.
   *
   * @return Relevant CounterUpdate or null if transformation failed.
   */
  @Override
  public CounterUpdate transform(MonitoringInfo monitoringInfo) {
    Optional<String> validationResult = validate(monitoringInfo);
    if (validationResult.isPresent()) {
      LOG.info(validationResult.get());
      return null;
    }

    long value = monitoringInfo.getMetric().getCounterData().getInt64Value();
    String urn = monitoringInfo.getUrn();

    final String ptransform = monitoringInfo.getLabelsMap().get(PTRANSFORM_LABEL);

    CounterStructuredNameAndMetadata name = new CounterStructuredNameAndMetadata();

    String nameWithNamespace = urn.substring(USER_COUNTER_URN_PREFIX.length()).replace("^:", "");

    final int lastColonIndex = nameWithNamespace.lastIndexOf(':');
    String counterName = nameWithNamespace.substring(lastColonIndex + 1);
    String counterNamespace =
        lastColonIndex == -1 ? "" : nameWithNamespace.substring(0, lastColonIndex);

    DataflowStepContext stepContext = transformIdMapping.get(ptransform);
    name.setName(
            new CounterStructuredName()
                .setOrigin(Origin.USER.toString())
                .setName(counterName)
                .setOriginalStepName(stepContext.getNameContext().originalName())
                .setOriginNamespace(counterNamespace))
        .setMetadata(new CounterMetadata().setKind("SUM"));

    return new CounterUpdate()
        .setStructuredNameAndMetadata(name)
        .setCumulative(true)
        .setInteger(DataflowCounterUpdateExtractor.longToSplitInt(value));
  }

  /** @return MonitoringInfo urns prefix that this transformer can convert to CounterUpdates. */
  public String getSupportedUrnPrefix() {
    return USER_COUNTER_URN_PREFIX;
  }
}
