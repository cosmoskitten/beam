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

package org.apache.beam.runners.direct;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/** A {@link KeyedResourcePool} which is limited to at most one outstanding instance at a time. */
class LockedKeyedResourcePool<K, V> implements KeyedResourcePool<K, V> {
  public static <K, V> LockedKeyedResourcePool<K, V> create() {
    return new LockedKeyedResourcePool<>();
  }

  private final ConcurrentMap<K, Optional<V>> cache;

  private LockedKeyedResourcePool() {
    cache = new ConcurrentHashMap<>();
  }

  @Override
  public Optional<V> tryAcquire(K key, Callable<V> loader) throws ExecutionException {
    Optional<V> evaluator = cache.replace(key, Optional.<V>absent());
    if (evaluator == null) {
      try {
        // No value already existed, so populate the cache with the value returned by the loader
        V value = loader.call();
        cache.putIfAbsent(key, Optional.of(value));
        // Some other thread may obtain the result after the putIfAbsent, so retry acquisition
        evaluator = cache.replace(key, Optional.<V>absent());
      } catch (Throwable t) {
        if (t instanceof Error) {
          throw new ExecutionError((Error) t);
        }
        if (t instanceof RuntimeException) {
          throw new UncheckedExecutionException(t);
        }
        throw new ExecutionException(t);
      }
    }
    return evaluator;
  }

  @Override
  public void release(K key, V value) {
    Optional<V> replaced = cache.replace(key, Optional.of(value));
    checkNotNull(replaced, "Tried to release before a value was acquired");
    checkState(
        !replaced.isPresent(),
        "Released a value to %s where there is already a value present. "
            + "At most one value may be present at a time.",
        LockedKeyedResourcePool.class.getSimpleName());
  }
}
