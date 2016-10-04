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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Test;

/**
 * Tests for {@link Distribution}.
 */
public class DistributionTest {

  private static final MetricName NAME = MetricName.named("test", "dist");

  @After
  public void tearDown() {
    MetricsEnvironment.unsetMetricsContainer();
  }

  @Test
  public void testReportWithoutContainer() {
    assertNull(MetricsEnvironment.getCurrentContainer());
    // Should not fail even though there is no metrics container.
    new Distribution(NAME).update(5L);
  }

  @Test
  public void testReportToCell() {
    MetricsContainer container = new MetricsContainer("step");
    MetricsEnvironment.setMetricsContainer(container);

    Distribution distribution = new Distribution(NAME);

    distribution.update(5L);

    DistributionCell cell = container.getOrCreateDistribution(NAME);
    assertThat(cell.getCumulative(), equalTo(DistributionData.create(5, 1, 5, 5)));

    distribution.update(36L);
    assertThat(cell.getCumulative(), equalTo(DistributionData.create(41, 2, 5, 36)));

    distribution.update(1L);
    assertThat(cell.getCumulative(), equalTo(DistributionData.create(42, 3, 1, 36)));
  }
}
