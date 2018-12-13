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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;

public class PCollectionConsumerRegistry {

  private ListMultimap<String, FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers;

  public PCollectionConsumerRegistry() {
    pCollectionIdsToConsumers = ArrayListMultimap.create();
  }

  public <T> FnDataReceiver<WindowedValue<T>> registerAndWrap(
      String pCollectionId, FnDataReceiver<WindowedValue<T>> consumer) {
    FnDataReceiver<WindowedValue<T>> wrappedReceiver =
        new ElementCountFnDataReceiver<T>(consumer, pCollectionId);
    pCollectionIdsToConsumers.put(pCollectionId, (FnDataReceiver) wrappedReceiver);
    return wrappedReceiver;
  }

  public Set<String> keySet() {
    return pCollectionIdsToConsumers.keySet();
  }

  public FnDataReceiver<WindowedValue<?>> getOnlyElement(String pCollectionId) {
    return Iterables.getOnlyElement(pCollectionIdsToConsumers.get(pCollectionId));
  }

  public List<FnDataReceiver<WindowedValue<?>>> get(String pCollectionId) {
    return pCollectionIdsToConsumers.get(pCollectionId);
  }
}
