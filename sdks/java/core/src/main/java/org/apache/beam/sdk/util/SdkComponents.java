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

package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.Components;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * SDK objects that will be represented at some later point within a {@link Components} object.
 */
public class SdkComponents {
  private final ImmutableBiMap<AppliedPTransform<?, ?, ?>, String> transformIds;
  private final ImmutableBiMap<PCollection<?>, String> pCollectionIds;
  private final ImmutableBiMap<WindowingStrategy<?, ?>, String> windowingStrategyIds;
  private final ImmutableBiMap<Coder<?>, String> coderIds;
  // TODO: Specify environments

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final BiMap<AppliedPTransform<?, ?, ?>, String> transformIds = HashBiMap.create();
    private final BiMap<PCollection<?>, String> collectionIds = HashBiMap.create();
    private final BiMap<WindowingStrategy<?, ?>, String> windowingStrategyIds = HashBiMap.create();
    private final BiMap<Coder<?>, String> coderIds = HashBiMap.create();

    private Builder() {
      // Might be nice when debugging, but not required for correctness
      windowingStrategyIds.put(WindowingStrategy.globalDefault(), "global_default");
    }

    /**
     * Puts the provided {@link AppliedPTransform} into this {@link SdkComponents}, using the full
     * name of the transform as the id.
     */
    public String putTransform(AppliedPTransform<?, ?, ?> pTransform) {
      if (transformIds.containsKey(pTransform)) {
        return transformIds.get(pTransform);
      }
      String name = pTransform.getFullName();
      if (name.isEmpty()) {
        name = "<unnamed_ptransform>";
      }
      transformIds.put(pTransform, name);
      return name;
    }

    /**
     * Puts the provided {@link PCollection} into this {@link SdkComponents} if it is not already
     * present, returning the id of the provided {@link PCollection} in this {@link SdkComponents}.
     *
     * <p>If the {@link PCollection} is not already present in this {@link SdkComponents} and the
     * provided ID has already been used to represent a {@link PCollection}, this method will throw
     * an {@link IllegalArgumentException}.
     */
    public String putPCollection(PCollection<?> pCollection, String id) {
      if (collectionIds.get(pCollection) != null) {
        return collectionIds.get(pCollection);
      }
      checkArgument(
          !collectionIds.containsValue(id),
          "Tried to insert %s with duplicate ID %s. Previous: %s",
          PCollection.class.getSimpleName(),
          id,
          coderIds.inverse().get(id));
      collectionIds.put(pCollection, id);
      return id;
    }

    /**
     * Puts the provided {@link WindowingStrategy} into this {@link SdkComponents} if it is not
     * already present, returning the id of the {@link WindowingStrategy} in this {@link
     * SdkComponents}.
     */
    public String putWindowingStrategy(WindowingStrategy<?, ?> windowingStrategy) {
      String existing = windowingStrategyIds.get(windowingStrategy);
      if (existing != null) {
        return existing;
      }
      String baseName = NameUtils.approximateSimpleName(windowingStrategy);
      String name = uniqify(baseName, windowingStrategyIds.values());
      windowingStrategyIds.put(windowingStrategy, name);
      return name;
    }

    /**
     * Puts the provided {@link Coder} into this {@link SdkComponents} if it is not already present,
     * returning the id of the {@link Coder} in this {@link SdkComponents}.
     */
    public String putCoder(Coder<?> coder) {
      String existing = coderIds.get(coder);
      if (existing != null) {
        return existing;
      }
      String baseName = NameUtils.approximateSimpleName(coder);
      String name = uniqify(baseName, coderIds.values());
      coderIds.put(coder, name);
      return name;
    }

    private String uniqify(String baseName, Set<String> existing) {
      String name = baseName;
      int increment = 1;
      while (existing.contains(name)) {
        name = baseName + Integer.toString(increment);
        increment++;
      }
      return name;
    }
  }

  private SdkComponents(
      BiMap<AppliedPTransform<?, ?, ?>, String> transformIds,
      BiMap<PCollection<?>, String> pCollectionIds,
      BiMap<WindowingStrategy<?, ?>, String> windowingStrategyIds,
      BiMap<Coder<?>, String> coderIds) {
    this.transformIds = ImmutableBiMap.copyOf(transformIds);
    this.pCollectionIds = ImmutableBiMap.copyOf(pCollectionIds);
    this.windowingStrategyIds = ImmutableBiMap.copyOf(windowingStrategyIds);
    this.coderIds = ImmutableBiMap.copyOf(coderIds);
  }

  public RunnerApi.Components toComponents() {
    throw new UnsupportedOperationException("Unimplemented");
  }

  /**
   * Returns the ID of the provided {@link AppliedPTransform transform}, throwing an
   * {@link IllegalArgumentException} if it is not present.
   */
  public String getTransformId(AppliedPTransform<?, ?, ?> transform) {
    checkArgument(
        transformIds.containsKey(transform),
        "Unknown %s %s",
        AppliedPTransform.class.getSimpleName(),
        transform);
    return transformIds.get(transform);
  }

  /**
   * Returns the ID of the provided {@link PCollection}, throwing an
   * {@link IllegalArgumentException} if it is not present.
   */
  public String getPCollectionId(PCollection<?> collection) {
    checkArgument(
        pCollectionIds.containsKey(collection),
        "Unknown %s %s",
        PCollection.class.getSimpleName(),
        collection);
    return pCollectionIds.get(collection);
  }

  /**
   * Returns the ID of the provided {@link WindowingStrategy}, throwing an exception if it is not
   * present.
   */
  public String getWindowingStrategyId(WindowingStrategy<?, ?> strategy) {
    checkArgument(
        windowingStrategyIds.containsKey(strategy),
        "Unknown %s %s",
        WindowingStrategy.class.getSimpleName(),
        strategy);
    return windowingStrategyIds.get(strategy);
  }

  /**
   * Returns the ID of the provided {@link Coder}, thrown an {@link IllegalArgumentException} if it
   * is not present.
   */
  public String getCoderId(Coder<?> coder) {
    checkArgument(coderIds.containsKey(coder), "Unknown %s %s", Coder.class.getSimpleName(), coder);
    return coderIds.get(coder);
  }
}
