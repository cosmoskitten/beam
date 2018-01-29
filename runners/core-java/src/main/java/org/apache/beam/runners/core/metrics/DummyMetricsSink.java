package org.apache.beam.runners.core.metrics;

import org.apache.beam.sdk.metrics.MetricQueryResults;

/**
 * This is the default Metrics Sink that just store in a static field the first counter (if it
 * exists) attempted value. This is usefull for tests.
 */
public class DummyMetricsSink extends MetricsSink<Long> {
  private static long counterValue;

  @Override protected MetricsSerializer<Long> provideSerializer() {
    return new MetricsSerializer<Long>() {

      @Override
      public Long serializeMetrics(MetricQueryResults metricQueryResults) throws Exception {
        return metricQueryResults.counters().iterator().hasNext()
            ? metricQueryResults.counters().iterator().next().attempted()
            : 0L;
      }
    };
  }

  @Override protected void writeSerializedMetrics(Long metrics) throws Exception {
    setCounterValue(metrics);
  }
  public static long getCounterValue(){
    return counterValue;
  }
  private static void setCounterValue(Long value){
    counterValue = value;
  }

  public static void clear(){
    counterValue = 0L;
  }
}
