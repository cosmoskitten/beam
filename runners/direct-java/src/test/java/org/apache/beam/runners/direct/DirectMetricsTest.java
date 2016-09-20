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

package org.apache.beam.runners.direct;

import static org.apache.beam.sdk.metrics.MetricMatchers.metricResult;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.metrics.MetricFilter;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricUpdates;
import org.apache.beam.sdk.metrics.MetricUpdates.MetricUpdate;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link DirectMetrics}.
 */
@RunWith(JUnit4.class)
public class DirectMetricsTest {

  private DirectMetrics metrics = new DirectMetrics();

  @Test
  public void testApplyLogicalQueryNoFilter() {
    metrics.applyLogical(MetricUpdates.create(
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("step1", "ns1", "name1"), 5L),
            MetricUpdate.create(MetricKey.create("step1", "ns1", "name2"), 8L))));
    metrics.applyLogical(MetricUpdates.create(
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("step2", "ns1", "name1"), 7L),
            MetricUpdate.create(MetricKey.create("step1", "ns1", "name2"), 4L))));

    assertThat(metrics.queryMetrics(MetricFilter.builder().build()).counters(), containsInAnyOrder(
        metricResult("ns1", "name1", "step1", 5L, 0L),
        metricResult("ns1", "name2", "step1", 12L, 0L),
        metricResult("ns1", "name1", "step2", 7L, 0L)));
  }

  @Test
  public void testApplyPhysicalQueryOneNamespace() {
    metrics.applyPhysical(MetricUpdates.create(
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("step1", "ns1", "name1"), 5L),
            MetricUpdate.create(MetricKey.create("step1", "ns2", "name1"), 8L))));
    metrics.applyPhysical(MetricUpdates.create(
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("step2", "ns1", "name1"), 7L),
            MetricUpdate.create(MetricKey.create("step1", "ns2", "name1"), 4L))));

    assertThat(metrics.queryMetrics(
        MetricFilter.builder().addName(MetricName.named("ns1", null)).build()).counters(),
        containsInAnyOrder(
            metricResult("ns1", "name1", "step1", 0L, 5L),
            metricResult("ns1", "name1", "step2", 0L, 7L)));
  }

  @Test
  public void testApplyPhysicalQueryCompositeScope() {
    metrics.applyPhysical(MetricUpdates.create(
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("Outer1/Inner1", "ns1", "name1"), 5L),
            MetricUpdate.create(MetricKey.create("Outer1/Inner2", "ns1", "name1"), 8L))));
    metrics.applyPhysical(MetricUpdates.create(
        ImmutableList.of(
            MetricUpdate.create(MetricKey.create("Outer1/Inner1", "ns1", "name1"), 7L),
            MetricUpdate.create(MetricKey.create("Outer2/Inner2", "ns1", "name1"), 4L))));

    assertThat(metrics.queryMetrics(
        MetricFilter.builder().addStep("Outer1").build()).counters(),
        containsInAnyOrder(
            metricResult("ns1", "name1", "Outer1/Inner1", 0L, 12L),
            metricResult("ns1", "name1", "Outer1/Inner2", 0L, 8L)));
  }
}
