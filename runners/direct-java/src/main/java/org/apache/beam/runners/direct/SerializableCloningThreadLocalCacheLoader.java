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

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.SerializableUtils;

import com.google.common.cache.CacheLoader;

import java.io.Serializable;

/**
 * A {@link CacheLoader} that loads {@link ThreadLocal ThreadLocals} with initial values equal to
 * the clone of the key.
 */
class SerializableCloningThreadLocalCacheLoader<K, T extends Serializable>
    extends CacheLoader<K, ThreadLocal<T>> {
  public static <T extends Serializable> SerializableCloningThreadLocalCacheLoader<T, T> create() {
    return new SerializableCloningThreadLocalCacheLoader<>(new IdentityFunction<T>());
  }

  private final SerializableFunction<K, T> mapper;

  private SerializableCloningThreadLocalCacheLoader(SerializableFunction<K, T> mapper) {
    this.mapper = mapper;
  }

  /**
   * Returns a {@link SerializableCloningThreadLocalCacheLoader} like this one, but which uses
   * the provided mapper to convert the key into the value to be cloned.
   */
  public <K, T extends Serializable> SerializableCloningThreadLocalCacheLoader<K, T> withMapper(
      SerializableFunction<K, T> mapper) {
    return new SerializableCloningThreadLocalCacheLoader<>(mapper);
  }

  @Override
  public ThreadLocal<T> load(K key) throws Exception {
    return new CloningThreadLocal<>(mapper.apply(key));
  }

  private static class CloningThreadLocal<T extends Serializable> extends ThreadLocal<T> {
    private final T original;

    public CloningThreadLocal(T value) {
      this.original = value;
    }

    @Override
    public T initialValue() {
      return SerializableUtils.clone(original);
    }
  }

  private static class IdentityFunction<T> implements SerializableFunction<T, T> {
    @Override
    public T apply(T input) {
      return input;
    }
  }
}
