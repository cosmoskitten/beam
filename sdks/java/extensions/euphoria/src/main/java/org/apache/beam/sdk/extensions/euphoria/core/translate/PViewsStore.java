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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * PViewsStore keeps track of {@link PCollection} to {@link PCollectionView} mappings created during
 * translation of Euphoria API into Beam java SDK. To prevent creating several {@link
 * PCollectionView PCollectionViews} to one {@link PCollection}.
 */
public class PViewsStore {

  private Map<Object, PCollectionView<?>> pViewsMap = new HashMap<>();

  /**
   * Creates new {@link PCollectionView} of given {@code pCollectionToView} iff there is no {@link
   * PCollectionView} already associated with {@code Key}.
   *
   * @param key mapping key
   * @param pCollectionToView
   * @param <K> element key type
   * @param <V> value key type
   * @return the current (already existing or computed) value associated with the specified key
   */
  @SuppressWarnings("unchecked")
  public <K, V> PCollectionView<Map<K, Iterable<V>>> computeViewAsMultimapIfAbsent(
      Object key, final PCollection<KV<K, V>> pCollectionToView) {
    return (PCollectionView<Map<K, Iterable<V>>>)
        pViewsMap.computeIfAbsent(key, (i) -> pCollectionToView.apply(View.asMultimap()));
  }
}
