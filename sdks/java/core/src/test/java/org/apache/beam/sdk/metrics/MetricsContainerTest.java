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

import static org.apache.beam.sdk.metrics.MetricMatchers.metricUpdate;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link MetricsContainer}.
 */
@RunWith(JUnit4.class)
public class MetricsContainerTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @After
  public void teardown() {
    MetricsContainer.unsetMetricsContainer();
  }

  @Test
  public void testUsesAppropriateMetricsContainer() {
    Counter counter = new Counter(MetricName.named("ns", "name"));
    MetricsContainer c1 = new MetricsContainer("step1");
    MetricsContainer c2 = new MetricsContainer("step2");

    MetricsContainer.setMetricsContainer(c1);
    counter.inc();
    MetricsContainer.setMetricsContainer(c2);
    counter.dec();
    MetricsContainer.unsetMetricsContainer();

    MetricUpdates updates1 = c1.getUpdates();
    MetricUpdates updates2 = c2.getUpdates();
    assertThat(updates1.counterUpdates(), contains(metricUpdate("ns", "name", "step1", 1L)));
    assertThat(updates2.counterUpdates(), contains(metricUpdate("ns", "name", "step2", -1L)));
  }

  @Test
  public void testBehavesWithoutMetricsContainer() {
    assertNull(MetricsContainer.getCurrentContainer());
  }

  @Test
  public void testCounterDeltas() {
    MetricsContainer container = new MetricsContainer("step1");
    CounterCell c1 = container.getOrCreateCounter(MetricName.named("ns", "name1"));
    CounterCell c2 = container.getOrCreateCounter(MetricName.named("ns", "name2"));
    assertThat("All counters should start out dirty",
        container.getUpdates().counterUpdates(), containsInAnyOrder(
        metricUpdate("name1", 0L),
        metricUpdate("name2", 0L)));
    container.commitUpdates();
    assertThat("After commit no counters should be dirty",
        container.getUpdates().counterUpdates(), emptyIterable());

    c1.add(5L);
    c2.add(4L);

    assertThat(container.getUpdates().counterUpdates(), containsInAnyOrder(
        metricUpdate("name1", 5L),
        metricUpdate("name2", 4L)));

    assertThat("Since we haven't committed, updates are still included",
        container.getUpdates().counterUpdates(), containsInAnyOrder(
        metricUpdate("name1", 5L),
        metricUpdate("name2", 4L)));

    container.commitUpdates();
    assertThat("After commit there are no updates",
        container.getUpdates().counterUpdates(), emptyIterable());

    c1.add(8L);
    assertThat(container.getUpdates().counterUpdates(), contains(
        metricUpdate("name1", 13L)));
  }

  @Test
  public void testCounterCumulatives() {
    MetricsContainer container = new MetricsContainer("step1");
    CounterCell c1 = container.getOrCreateCounter(MetricName.named("ns", "name1"));
    CounterCell c2 = container.getOrCreateCounter(MetricName.named("ns", "name2"));
    c1.add(2L);
    c2.add(4L);
    c1.add(3L);

    container.getUpdates();
    container.commitUpdates();
    assertThat("Committing updates shouldn't affect cumulative counter values",
        container.getCumulative().counterUpdates(), containsInAnyOrder(
        metricUpdate("name1", 5L),
        metricUpdate("name2", 4L)));

    c1.add(8L);
    assertThat(container.getCumulative().counterUpdates(), containsInAnyOrder(
        metricUpdate("name1", 13L),
        metricUpdate("name2", 4L)));
  }

  @Test
  public void testDistributionDeltas() {
    MetricsContainer container = new MetricsContainer("step1");
    DistributionCell c1 = container.getOrCreateDistribution(MetricName.named("ns", "name1"));
    DistributionCell c2 = container.getOrCreateDistribution(MetricName.named("ns", "name2"));

    assertThat("Initial update includes initial zero-values",
        container.getUpdates().distributionUpdates(), containsInAnyOrder(
        metricUpdate("name1", DistributionData.EMPTY),
        metricUpdate("name2", DistributionData.EMPTY)));

    container.commitUpdates();
    assertThat("No updates after commit",
        container.getUpdates().distributionUpdates(), emptyIterable());

    c1.report(5L);
    c2.report(4L);

    assertThat(container.getUpdates().distributionUpdates(), containsInAnyOrder(
        metricUpdate("name1", DistributionData.create(5, 1, 5, 5)),
        metricUpdate("name2", DistributionData.create(4, 1, 4, 4))));
    assertThat("Updates stay the same without commit",
        container.getUpdates().distributionUpdates(), containsInAnyOrder(
        metricUpdate("name1", DistributionData.create(5, 1, 5, 5)),
        metricUpdate("name2", DistributionData.create(4, 1, 4, 4))));

    container.commitUpdates();
    assertThat("No updatess after commit",
        container.getUpdates().distributionUpdates(), emptyIterable());

    c1.report(8L);
    c1.report(4L);
    assertThat(container.getUpdates().distributionUpdates(), contains(
        metricUpdate("name1", DistributionData.create(17, 3, 4, 8))));
    container.commitUpdates();
  }
}
