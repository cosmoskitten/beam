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
