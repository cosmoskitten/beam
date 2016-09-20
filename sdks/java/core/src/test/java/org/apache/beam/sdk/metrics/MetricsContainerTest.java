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
import static org.junit.Assert.assertThat;

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

  @Test
  public void testUsesAppropriateMetricsContainer() {
    Counter counter = new Counter(MetricName.named("ns", "name"));
    MetricsContainer c1 = new MetricsContainer("step1");
    MetricsContainer c2 = new MetricsContainer("step2");

    MetricsContainer.setMetricsContainer(c1);
    counter.inc();
    MetricsContainer.setMetricsContainer(c2);
    counter.dec();
    MetricsContainer.unsetMetricsContainer();;

    MetricUpdates updates1 = c1.getDeltas();
    MetricUpdates updates2 = c2.getDeltas();
    assertThat(updates1.counterUpdates(), contains(metricUpdate("ns", "name", "step1", 1L)));
    assertThat(updates2.counterUpdates(), contains(metricUpdate("ns", "name", "step2", -1L)));
  }

  @Test
  public void testFailsWithoutMetricsContainer() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("setMetricsContainer");
    MetricsContainer.getCurrentContainer();
  }

  @Test
  public void testCounterDeltasAndCumulatives() {
    MetricsContainer container = new MetricsContainer("step1");
    CounterCell c1 = container.getOrCreateCounter(MetricName.named("ns", "name1"));
    CounterCell c2 = container.getOrCreateCounter(MetricName.named("ns", "name2"));

    MetricUpdates deltas = container.getDeltas();
    // All counters should start out dirty
    assertThat(deltas.counterUpdates(), containsInAnyOrder(
        metricUpdate("ns", "name1", "step1", 0L),
        metricUpdate("ns", "name2", "step1", 0L)));
    // Committing should cause the deltas to be clean
    container.commitDeltas(deltas);
    deltas = container.getDeltas();
    assertThat(deltas.counterUpdates(), emptyIterable());

    c1.add(5L);
    c2.add(4L);

    deltas = container.getDeltas();
    assertThat(deltas.counterUpdates(), containsInAnyOrder(
        metricUpdate("ns", "name1", "step1", 5L),
        metricUpdate("ns", "name2", "step1", 4L)));
    assertThat(container.getCumulative().counterUpdates(), containsInAnyOrder(
        metricUpdate("ns", "name1", "step1", 5L),
        metricUpdate("ns", "name2", "step1", 4L)));

    // Since we haven't committed yet, the delta is the same
    deltas = container.getDeltas();
    assertThat(deltas.counterUpdates(), containsInAnyOrder(
        metricUpdate("ns", "name1", "step1", 5L),
        metricUpdate("ns", "name2", "step1", 4L)));

    // When we commit, the deltas should be empty again, while the cumulative is still the same.
    container.commitDeltas(deltas);
    deltas = container.getDeltas();
    assertThat(deltas.counterUpdates(), emptyIterable());
    assertThat(container.getCumulative().counterUpdates(), containsInAnyOrder(
        metricUpdate("ns", "name1", "step1", 5L),
        metricUpdate("ns", "name2", "step1", 4L)));

    c1.add(8L);
    deltas = container.getDeltas();
    assertThat(deltas.counterUpdates(), contains(
        metricUpdate("ns", "name1", "step1", 8L)));
    assertThat(container.getCumulative().counterUpdates(), containsInAnyOrder(
        metricUpdate("ns", "name1", "step1", 13L),
        metricUpdate("ns", "name2", "step1", 4L)));
  }

}
