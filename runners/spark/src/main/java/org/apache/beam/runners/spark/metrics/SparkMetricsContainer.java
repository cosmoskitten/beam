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

package org.apache.beam.runners.spark.metrics;

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.beam.runners.spark.metrics.MetricAggregator.CounterAggregator;
import org.apache.beam.runners.spark.metrics.MetricAggregator.DistributionAggregator;
import org.apache.beam.sdk.metrics.DistributionData;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricUpdates;
import org.apache.beam.sdk.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Spark accumulator value which holds all {@link MetricsContainer}s, aggregates and merges them.
 */
public class SparkMetricsContainer implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SparkMetricsContainer.class);

  private transient volatile LoadingCache<String, MetricsContainer> metricsContainers;

  private final Map<MetricKey, CounterAggregator> counters = new HashMap<>();
  private final Map<MetricKey, DistributionAggregator> distributions = new HashMap<>();

  SparkMetricsContainer() {}

  public static Accumulator<SparkMetricsContainer> getAccumulator(JavaSparkContext jsc) {
    return MetricsAccumulator.getInstance(jsc);
  }

  public MetricsContainer getContainer(String stepName) {
    if (metricsContainers == null) {
      synchronized (this) {
        if (metricsContainers == null) {
          metricsContainers = CacheBuilder.<String, SparkMetricsContainer>newBuilder()
              .build(new MetricsContainerCacheLoader());
        }
      }
    }
    try {
      return metricsContainers.get(stepName);
    } catch (ExecutionException e) {
      LOG.error("Error while creating metrics container", e);
      return null;
    }
  }

  Collection<CounterAggregator> getCounters() {
    return counters.values();
  }

  Collection<DistributionAggregator> getDistributions() {
    return distributions.values();
  }

  SparkMetricsContainer merge(SparkMetricsContainer other) {
    SparkMetricsContainer merged = new SparkMetricsContainer();
    merged.aggregate(this.counters.values(), this.distributions.values());
    merged.aggregate(other.counters.values(), other.distributions.values());
    return merged;
  }

  private void writeObject (ObjectOutputStream out) throws IOException {
    aggregate();
    out.defaultWriteObject();
  }

  private void aggregate() {
    if (metricsContainers != null) {
      for (MetricsContainer container : metricsContainers.asMap().values()) {
        MetricUpdates cumulative = container.getCumulative();
        Iterable<CounterAggregator> counterAggregators =
            Iterables.transform(cumulative.counterUpdates(),
                new Function<MetricUpdate<Long>, CounterAggregator>() {
                  @SuppressWarnings("ConstantConditions")
                  @Nullable
                  @Override
                  public CounterAggregator
                  apply(MetricUpdate<Long> update) {
                    return new CounterAggregator(new SparkMetricKey(update.getKey()),
                        update.getUpdate());
                  }
                });
        Iterable<DistributionAggregator> distributionAggregators =
            Iterables.transform(cumulative.distributionUpdates(),
                new Function<MetricUpdate<DistributionData>, DistributionAggregator>() {
                  @SuppressWarnings("ConstantConditions")
                  @Nullable
                  @Override
                  public DistributionAggregator
                  apply(MetricUpdate<DistributionData> update) {
                    return new DistributionAggregator(new SparkMetricKey(update.getKey()),
                        new SparkDistributionData(update.getUpdate()));
                  }
                });
        aggregate(counterAggregators, distributionAggregators);
      }
    }
  }

  private void aggregate(Iterable<CounterAggregator> counterUpdates,
                         Iterable<DistributionAggregator> distributionUpdates) {
    for (CounterAggregator update : counterUpdates) {
      MetricKey key = update.getKey();
      CounterAggregator current = counters.get(key);
      Long counterUpdate = update.getValue();
      CounterAggregator newValue;
      if (current != null) {
        newValue = current.updated(counterUpdate);
      } else {
        newValue = new CounterAggregator(key, counterUpdate);
      }
      counters.put(new SparkMetricKey(key), newValue);
    }
    for (DistributionAggregator update : distributionUpdates) {
      MetricKey key = update.getKey();
      DistributionAggregator current = distributions.get(key);
      DistributionData distributionUpdate = update.getValue();
      DistributionAggregator newValue;
      if (current != null) {
        newValue = current.updated(distributionUpdate);
      } else {
        newValue = new DistributionAggregator(key, distributionUpdate);
      }
      distributions.put(new SparkMetricKey(key), newValue);
    }
  }

  private static class MetricsContainerCacheLoader extends CacheLoader<String, MetricsContainer> {
    @SuppressWarnings("NullableProblems")
    @Override
    public MetricsContainer load(String stepName) throws Exception {
      return new MetricsContainer(stepName);
    }
  }

  private static class SparkDistributionData extends DistributionData implements Serializable {
    private final long sum;
    private final long count;
    private final long min;
    private final long max;

    SparkDistributionData(DistributionData original) {
      this.sum = original.sum();
      this.count = original.count();
      this.min = original.min();
      this.max = original.max();
    }

    @Override
    public long sum() {
      return sum;
    }

    @Override
    public long count() {
      return count;
    }

    @Override
    public long min() {
      return min;
    }

    @Override
    public long max() {
      return max;
    }
  }

  private static class SparkMetricKey extends MetricKey implements Serializable {
    private final String stepName;
    private final MetricName metricName;

    SparkMetricKey(MetricKey original) {
      this.stepName = original.stepName();
      MetricName metricName = original.metricName();
      this.metricName = new SparkMetricName(metricName.namespace(), metricName.name());
    }

    @Override
    public String stepName() {
      return stepName;
    }

    @Override
    public MetricName metricName() {
      return metricName;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof MetricKey) {
        MetricKey that = (MetricKey) o;
        return (this.stepName.equals(that.stepName()))
            && (this.metricName.equals(that.metricName()));
      }
      return false;
    }

    @Override
    public int hashCode() {
      int h = 1;
      h *= 1000003;
      h ^= stepName.hashCode();
      h *= 1000003;
      h ^= metricName.hashCode();
      return h;
    }
  }

  private static class SparkMetricName extends MetricName implements Serializable {
    private final String namespace;
    private final String name;

    SparkMetricName(String namespace, String name) {
      this.namespace = namespace;
      this.name = name;
    }

    @Override
    public String namespace() {
      return namespace;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof MetricName) {
        MetricName that = (MetricName) o;
        return (this.namespace.equals(that.namespace()))
            && (this.name.equals(that.name()));
      }
      return false;
    }

    @Override
    public int hashCode() {
      int h = 1;
      h *= 1000003;
      h ^= namespace.hashCode();
      h *= 1000003;
      h ^= name.hashCode();
      return h;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, ?> metric :  new SparkBeamMetric().renderAll().entrySet()) {
      sb.append(metric.getKey()).append(": ").append(metric.getValue()).append("\n");
    }
    return sb.toString();
  }
}
