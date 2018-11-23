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
package org.apache.beam.runners.spark.util;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Cache deserialized side inputs for executor so every task doesnt need to deserialize them again.
 * Side inputs are stored in {@link WeakHashMap} so if there is no reference to a key, sideInput is
 * garbage collected.
 */
public class SideInputStorage {

  /** JVM deserialized side input cache */
  private static final Map<Key<?>, ?> materializedSideInputs =
      Collections.synchronizedMap(new WeakHashMap<>());

  public static Map<Key<?>, ?> getMaterializedSideInputs() {
    return materializedSideInputs;
  }

  /**
   * Composite key of {@link PCollectionView} and {@link BoundedWindow} used to identify
   * materialized results.
   *
   * @param <T> type of result
   */
  public static class Key<T> {

    private final PCollectionView<T> view;
    private final BoundedWindow window;

    Key(PCollectionView<T> view, BoundedWindow window) {
      this.view = view;
      this.window = window;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Key<?> key = (Key<?>) o;
      return Objects.equals(view, key.view) && Objects.equals(window, key.window);
    }

    @Override
    public int hashCode() {
      return Objects.hash(view, window);
    }
  }
}
