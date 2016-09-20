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

import com.google.common.collect.Iterables;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * A map from {@code K} to {@code T} that supports getting or creating values associated with a key
 * in a thread-safe manner.
 *
 * <p>All instances must override {@link #createInstance()}.
 */
@Experimental(Kind.METRICS)
public abstract class MetricsMap<K, T> {

  private final ConcurrentMap<K, T> metrics = new ConcurrentHashMap<>();

  public MetricsMap() {}

  /**
   * Get or create the value associated with the given key.
   */
  public T getOrCreate(K key) {
    T metric = metrics.get(key);
    if (metric == null) {
      metric = createInstance();
      if (metrics.putIfAbsent(key, metric) == null) {
        metric = metrics.get(key);
      }
    }
    return metric;
  }

  /**
   * Get the value associated with the given key, if it exists.
   */
  @Nullable
  public T get(K key) {
    return metrics.get(key);
  }

  /**
   * Return an iterator over all the entries in the given set.
   */
  public Iterable<Map.Entry<K, T>> entries() {
    return Iterables.unmodifiableIterable(metrics.entrySet());
  }

  protected abstract T createInstance();
}
