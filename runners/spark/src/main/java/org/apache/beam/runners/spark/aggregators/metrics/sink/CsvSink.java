package org.apache.beam.runners.spark.aggregators.metrics.sink;

import org.apache.beam.runners.spark.aggregators.metrics.AggregatorMetric;
import org.apache.beam.runners.spark.aggregators.metrics.WithNamedAggregatorsSupport;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;

import org.apache.spark.metrics.sink.Sink;

import java.io.File;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;




/**
 * A Spark {@link Sink} that is tailored to report {@link AggregatorMetric} metrics
 * to a CSV file.
 */
public class CsvSink implements Sink {

  private final CsvReporter metricReporter;
  private final TimeUnit timeUnit;
  private final Long period;

  public CsvSink(final Properties properties,
                 final MetricRegistry metricRegistry,
                 final org.apache.spark.SecurityManager securityMgr) {

    period = Long.parseLong(properties.getProperty("period"));
    timeUnit = TimeUnit.valueOf(properties.getProperty("unit", "SECONDS").toUpperCase());
    metricReporter =
      CsvReporter.forRegistry(WithNamedAggregatorsSupport.forRegistry(metricRegistry))
        .formatFor(Locale.US)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .convertRatesTo(TimeUnit.SECONDS)
        .build(new File(properties.getProperty("directory")));
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
