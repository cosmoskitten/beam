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

package org.apache.beam.runners.core.construction;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.Components;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;

/** SDK objects that will be represented at some later point within a {@link Components} object. */
public class RehydratedComponents {
  private final Components components;

  private final Map<String, PCollection<?>> pCollections;

  private final LoadingCache<String, WindowingStrategy<?, ?>> windowingStrategies =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<String, WindowingStrategy<?, ?>>() {
                @Override
                public WindowingStrategy<?, ?> load(String id) throws Exception {
                  return WindowingStrategyTranslation.fromProto(
                      components.getWindowingStrategiesOrThrow(id), RehydratedComponents.this);
                }
              });

  private final LoadingCache<String, Coder<?>> coders =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<String, Coder<?>>() {
                @Override
                public Coder<?> load(String id) throws Exception {
                  return CoderTranslation.fromProto(
                      components.getCodersOrThrow(id), RehydratedComponents.this);
                }
              });

  /** Create a new {@link RehydratedComponents} with no components. */
  public static RehydratedComponents create(RunnerApi.Components components) {
    return new RehydratedComponents(components);
  }

  private RehydratedComponents(RunnerApi.Components components) {
    this.components = components;
    this.pCollections = new HashMap<>();
  }

  /**
   * Gets the PCollection for the given ID, or rehydrates it from the components.
   */
  PCollection<?> getPCollection(String pCollectionId, Pipeline pipeline) throws IOException {
    PCollection<?> pCollection = pCollections.get(pCollectionId);
    if (pCollection != null) {
      return pCollection;
    }

    pCollection =
        PCollectionTranslation.fromProto(
            components.getPcollectionsOrThrow(pCollectionId), pipeline, this);
    pCollection.setName(pCollectionId);
    pCollections.put(pCollectionId, pCollection);
    return pCollection;
  }

  /**
   * Registers the provided {@link WindowingStrategy} into this {@link RehydratedComponents},
   * returning a unique ID for the {@link WindowingStrategy}. Multiple registrations of the same
   * {@link WindowingStrategy} will return the same unique ID.
   */
  WindowingStrategy<?, ?> getWindowingStrategy(String windowingStrategyId) throws IOException {
    try {
      return windowingStrategies.get(windowingStrategyId);
    } catch (ExecutionException exc) {
      throw new RuntimeException(exc);
    }
  }

  /**
   * ...
   */
  Coder<?> getCoder(String coderId) throws IOException {
    try {
      return coders.get(coderId);
    } catch (ExecutionException exc) {
      throw new RuntimeException(exc);
    }
  }
}
