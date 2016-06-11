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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.WindowedValue;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

/**
 * {@link KeySelector} that retrieves a key from a {@link KeyedWorkItem}.
 */
public class WorkItemKeySelector<K, V>
    implements KeySelector<WindowedValue<SingletonKeyedWorkItem<K, V>>, K>, ResultTypeQueryable<K> {

  private final boolean isKeyVoid;
  private final Coder<K> keyCoder;

  public WorkItemKeySelector(Coder<K> keyCoder) {
    this.isKeyVoid = keyCoder instanceof VoidCoder;
    this.keyCoder = keyCoder;
  }

  @Override
  public K getKey(WindowedValue<SingletonKeyedWorkItem<K, V>> value) throws Exception {
    return isKeyVoid ? (K) VoidValue.instance: value.getValue().key();
  }

  @Override
  public TypeInformation<K> getProducedType() {
    return new CoderTypeInformation<>(keyCoder);
  }

  /**
   * Special type to return for null keys because Flink does not allow null keys.
   */
  public static class VoidValue {
    private VoidValue() {}

    public static VoidValue instance = new VoidValue();
  }

}
