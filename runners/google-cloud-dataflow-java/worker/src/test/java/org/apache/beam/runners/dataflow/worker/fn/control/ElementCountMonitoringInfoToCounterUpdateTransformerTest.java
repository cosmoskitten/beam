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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.SpecMonitoringInfoValidator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ElementCountMonitoringInfoToCounterUpdateTransformerTest {

  @Rule public final ExpectedException exception = ExpectedException.none();

  @Mock private SpecMonitoringInfoValidator mockSpecValidator;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void tesTransformReturnsNullIfSpecValidationFails() {
    Map<String, String> pcollectionNameMapping = new HashMap<>();
    ElementCountMonitoringInfoToCounterUpdateTransformer testObject =
        new ElementCountMonitoringInfoToCounterUpdateTransformer(
            mockSpecValidator, pcollectionNameMapping);
    Optional<String> error = Optional.of("Error text");
    when(mockSpecValidator.validate(any())).thenReturn(error);
    assertEquals(null, testObject.transform(null));
  }

  @Test
  public void testTransformThrowsIfMonitoringInfoWithWrongUrnPrefixReceived() {
    Map<String, String> pcollectionNameMapping = new HashMap<>();
    MonitoringInfo monitoringInfo =
        MonitoringInfo.newBuilder().setUrn("beam:user:metric:element_count:v1").build();
    ElementCountMonitoringInfoToCounterUpdateTransformer testObject =
        new ElementCountMonitoringInfoToCounterUpdateTransformer(
            mockSpecValidator, pcollectionNameMapping);
    when(mockSpecValidator.validate(any())).thenReturn(Optional.empty());

    exception.expect(RuntimeException.class);
    testObject.transform(monitoringInfo);
  }

  @Test
  public void testTransformReturnsNullIfMonitoringInfoWithUnknownPCollectionLabelPresent() {
    Map<String, String> pcollectionNameMapping = new HashMap<>();
    pcollectionNameMapping.put("knownValue", "anyTransformedValue");
    MonitoringInfo monitoringInfo =
        MonitoringInfo.newBuilder()
            .setUrn("beam:metric:element_count:v1")
            .putLabels("PCOLLECTION", "anyValue")
            .build();
    ElementCountMonitoringInfoToCounterUpdateTransformer testObject =
        new ElementCountMonitoringInfoToCounterUpdateTransformer(
            mockSpecValidator, pcollectionNameMapping);
    when(mockSpecValidator.validate(any())).thenReturn(Optional.empty());
    assertEquals(null, testObject.transform(monitoringInfo));
  }

  @Test
  public void testTransformReturnsValidCounterUpdateWhenValidMonitoringInfoReceived() {
    Map<String, String> pcollectionNameMapping = new HashMap<>();
    pcollectionNameMapping.put("anyValue", "transformedValue");

    MonitoringInfo monitoringInfo =
        MonitoringInfo.newBuilder()
            .setUrn("beam:metric:element_count:v1")
            .putLabels("PCOLLECTION", "anyValue")
            .build();
    ElementCountMonitoringInfoToCounterUpdateTransformer testObject =
        new ElementCountMonitoringInfoToCounterUpdateTransformer(
            mockSpecValidator, pcollectionNameMapping);
    when(mockSpecValidator.validate(any())).thenReturn(Optional.empty());

    CounterUpdate result = testObject.transform(monitoringInfo);
    assertNotEquals(null, result);

    assertEquals(
        "{cumulative=true, integer={highBits=0, lowBits=0}, "
            + "nameAndKind={kind=SUM, "
            + "name=transformedValue-ElementCount}}",
        result.toString());
  }
}
