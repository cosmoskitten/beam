package org.apache.beam.runners.core.metrics;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


/** Tests for {@link SimpleMonitoringInfoBuilder}. */
@RunWith(JUnit4.class)
public class SimpleMonitoringInfoBuilderTest {

  @Test
  public void testSpecNotFullyMet() {
    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder(true);
    builder.setUrn(SimpleMonitoringInfoBuilder.ELEMENT_COUNT_URN);
    assertNull(builder.build());

    builder.setInt64Value(1);
    assertNull(builder.build());

    builder.setPTransformLabel("myTransform");
    assertNull(builder.build());

    builder.setPCollectionLabel("myPcollection");
    assertTrue(builder.build() != null);
  }

  @Test
  public void testSpecNotFullyMetUserCounter() {
    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder(true);
    builder.setUrnForUserMetric("myNamespace", "myName");
    assertNull(builder.build());

    builder.setInt64Value(1);
    assertTrue(builder.build() != null);
  }

  @Test
  public void testUserMetricWithInvalidDelimiterCharacter() {
    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder(true);
    builder.setUrnForUserMetric("myNamespace:withInvalidChar", "myName");
    builder.setInt64Value(1);
    assertNull(builder.build());
  }
}
