package org.apache.beam.runners.spark.aggregators.metrics.sink;

import org.apache.beam.runners.spark.aggregators.metrics.AggregatorMetric;
import org.apache.beam.runners.spark.aggregators.metrics.WithNamedAggregatorsSupport;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import org.apache.spark.metrics.sink.Sink;

import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A Spark {@link Sink} that is tailored to report {@link AggregatorMetric} metrics
 * to Graphite.
 */
public class GraphiteSink implements Sink {

  private final GraphiteReporter metricReporter;
  private final TimeUnit timeUnit;
  private final Long period;

  public GraphiteSink(final Properties properties,
                      final MetricRegistry metricRegistry,
                      final org.apache.spark.SecurityManager securityMgr) {

    period = Long.parseLong(properties.getProperty("period"));
    timeUnit = TimeUnit.valueOf(properties.getProperty("unit", "SECONDS").toUpperCase());
    final String host = properties.getProperty("host");
    final int port = Integer.parseInt(properties.getProperty("port"));
    metricReporter =
      GraphiteReporter
        .forRegistry(WithNamedAggregatorsSupport.forRegistry(metricRegistry))
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .convertRatesTo(TimeUnit.SECONDS)
        .prefixedWith("")
        .build(new Graphite(new InetSocketAddress(host, port)));
  }

  @Override
  public void start() {
    metricReporter.start(period, timeUnit);
  }

  @Override
  public void stop() {
    metricReporter.stop();
  }

  @Override
  public void report() {
    metricReporter.report();
  }
}
