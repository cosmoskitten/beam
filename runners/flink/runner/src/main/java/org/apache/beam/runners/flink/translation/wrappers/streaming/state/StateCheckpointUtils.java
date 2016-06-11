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
package org.apache.beam.runners.flink.translation.wrappers.streaming.state;

import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.state.StateNamespace;
import org.apache.beam.sdk.util.state.StateNamespaces;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StateCheckpointUtils {

  public static <K> void encodeState(Map<K, FlinkStateInternals<K>> perKeyStateInternals,
               StateCheckpointWriter writer, Coder<K> keyCoder) throws IOException {
    CoderTypeSerializer<K> keySerializer = new CoderTypeSerializer<>(keyCoder);

    int noOfKeys = perKeyStateInternals.size();
    writer.writeInt(noOfKeys);
    for (Map.Entry<K, FlinkStateInternals<K>> keyStatePair : perKeyStateInternals.entrySet()) {
      K key = keyStatePair.getKey();
      FlinkStateInternals<K> state = keyStatePair.getValue();

      // encode the key
      writer.serializeKey(key, keySerializer);

      // write the associated state
      state.persistState(writer);
    }
  }

  public static <K> Map<K, FlinkStateInternals<K>> decodeState(
      StateCheckpointReader reader,
      OutputTimeFn<? super BoundedWindow> outputTimeFn,
      Coder<K> keyCoder,
      Coder<? extends BoundedWindow> windowCoder,
      ClassLoader classLoader) throws IOException, ClassNotFoundException {

    int noOfKeys = reader.getInt();
    Map<K, FlinkStateInternals<K>> perKeyStateInternals = new HashMap<>(noOfKeys);
    perKeyStateInternals.clear();

    CoderTypeSerializer<K> keySerializer = new CoderTypeSerializer<>(keyCoder);
    for (int i = 0; i < noOfKeys; i++) {

      // decode the key.
      K key = reader.deserializeKey(keySerializer);

      //decode the state associated to the key.
      FlinkStateInternals<K> stateForKey =
          new FlinkStateInternals<>(key, windowCoder, outputTimeFn);
      stateForKey.restoreState(reader, classLoader);
      perKeyStateInternals.put(key, stateForKey);
    }
    return perKeyStateInternals;
  }
}
