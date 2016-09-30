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

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link Distribution}.
 */
public class DistributionTest {

  private static final MetricName NAME = MetricName.named("test", "dist");

  @After
  public void tearDown() {
    MetricsContainer.unsetMetricsContainer();
  }

  @Test
  public void testReportWithoutContainer() {
    assertNull(MetricsContainer.getCurrentContainer());
    // Should not fail even though there is no metrics container.
    new Distribution(NAME).report(5L);
  }

  @Test
  public void testReportToCell() {
    MetricsContainer container = Mockito.mock(MetricsContainer.class);
    DistributionCell cell = Mockito.mock(DistributionCell.class);
    when(container.getOrCreateDistribution(NAME)).thenReturn(cell);
    MetricsContainer.setMetricsContainer(container);

    Distribution distribution = new Distribution(NAME);
    distribution.report(5L);
    verify(cell).report(5L);

    distribution.report(42L);
    verify(cell).report(42L);
  }
}
