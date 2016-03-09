/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.common.collect.Ordering;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@link PriorityQueue} that is partitioned by key and thread-safe.
 */
public class ConcurrentPartitionedPriorityQueue<K, V> {
  public static <K, V> ConcurrentPartitionedPriorityQueue<K, V> create(Comparator<V> comparator) {
    return new ConcurrentPartitionedPriorityQueue<>(Ordering.from(comparator));
  }

  private final Ordering<V> ordering;
  private final ConcurrentMap<K, PriorityQueue<V>> queues;

  public ConcurrentPartitionedPriorityQueue(Ordering<V> ordering) {
    this.ordering = ordering;
    queues = new ConcurrentHashMap<>();
  }

  /**
   * Place the provided value in the priority queue belonging to the key.
   */
  public void offer(K key, V value) {
    PriorityQueue<V> queue = queues.get(key);
    if (queue == null) {
      queue = new PriorityQueue<>(ordering);
      queues.putIfAbsent(key, queue);
      queue = queues.get(key);
    }
    synchronized (queue) {
      queue.offer(value);
    }
  }

  /**
   * Retrieve the head of the queue for the provided key if it satisfies the provided prediate.
   */
  public V pollIfSatisfies(K key, SerializableFunction<V, Boolean> toSatisfy) {
    PriorityQueue<V> queue = queues.get(key);
    if (queue == null) {
      return null;
    }
    synchronized (queue) {
      if (!queue.isEmpty() && toSatisfy.apply(queue.peek())) {
        return queue.poll();
      }
      return null;
    }
  }
}
