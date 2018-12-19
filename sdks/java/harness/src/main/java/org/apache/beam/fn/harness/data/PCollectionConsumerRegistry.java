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
package org.apache.beam.fn.harness.data;

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * The {@code PCollectionConsumerRegistry} is used to maintain a collection of consuming
 * FnDataReceiver for each pCollectionId. Registering with this class allows inserting an element
 * count counter for every pCollection.
 */
public class PCollectionConsumerRegistry {

  private ListMultimap<String, FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers;
  private Map<String, ElementCountFnDataReceiver> pCollectionIdsToWrappedConsumer;

  public PCollectionConsumerRegistry() {
    pCollectionIdsToConsumers = ArrayListMultimap.create();
    pCollectionIdsToWrappedConsumer = new HashMap<String, ElementCountFnDataReceiver>();
  }

  public <T> void register(String pCollectionId, FnDataReceiver<WindowedValue<T>> consumer) {
    // Just save these consumers for now, but package them up later with an
    // ElementCountFnDataReceiver and possibly a MultiplexingFnDataReceiver
    // if there are multiple consumers.
    ElementCountFnDataReceiver wrappedConsumer =
        pCollectionIdsToWrappedConsumer.getOrDefault(pCollectionId, null);
    if (wrappedConsumer != null) {
      throw new RuntimeException(
          "New consumers for a pCollectionId cannot be register()-d after "
              + "calling getMultiplexingConsumer.");
    }
    pCollectionIdsToConsumers.put(pCollectionId, (FnDataReceiver) consumer);
  }

  /** @return the list of pcollection ids. */
  public Set<String> keySet() {
    return pCollectionIdsToConsumers.keySet();
  }

  /** @return the only FnDataReceiver for the pcollection. */
  public FnDataReceiver<WindowedValue<?>> getOnlyElement(String pCollectionId) {
    return Iterables.getOnlyElement(pCollectionIdsToConsumers.get(pCollectionId));
  }

  /**
   * New consumers should not be added after calling this method. This will cause a
   * RuntimeException, as this would fail to properly wrap the late-added consumer to the
   * ElementCountFnDataReceiver.
   *
   * @return A single ElementCountFnDataReceiver which directly wraps all the registered consumers,
   *     possibly using a MultiplexingFnDataReceiver.
   */
  public FnDataReceiver<WindowedValue<?>> getMultiplexingConsumer(String pCollectionId) {
    ElementCountFnDataReceiver wrappedConsumer =
        pCollectionIdsToWrappedConsumer.getOrDefault(pCollectionId, null);
    if (wrappedConsumer == null) {
      List<FnDataReceiver<WindowedValue<?>>> consumers =
          pCollectionIdsToConsumers.get(pCollectionId);
      FnDataReceiver<WindowedValue<?>> consumer =
          MultiplexingFnDataReceiver.forConsumers(consumers);
      wrappedConsumer = new ElementCountFnDataReceiver(consumer, pCollectionId);
      pCollectionIdsToWrappedConsumer.put(pCollectionId, wrappedConsumer);
    }
    return wrappedConsumer;
  }

  /**
   * @return the number of underlying consumers for a pCollectionId, some tests may wish to check
   *     this.
   */
  @VisibleForTesting
  public List<FnDataReceiver<WindowedValue<?>>> getUnderlyingConsumers(String pCollectionId) {
    return pCollectionIdsToConsumers.get(pCollectionId);
  }
}
