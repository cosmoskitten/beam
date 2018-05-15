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

package org.apache.beam.runners.fnexecution.state;

import java.util.Iterator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.common.Reiterable;

public class StateRequestHandlers {

  public interface MultimapSideInputHandler<K, V, W extends BoundedWindow> {
    Reiterable<V> get(K key, W window);
  }

  public interface MultimapSideInputHandlerFactory {
    <K, V, W extends BoundedWindow> MultimapSideInputHandler<K, V, W> forSideInput(
        String pTransformId,
        String sideInputId,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        Coder<W> windowCoder);

    /**
     * Throws a {@link UnsupportedOperationException} on the first access
     */
    static MultimapSideInputHandlerFactory unsupported() {
      return new MultimapSideInputHandlerFactory() {
        @Override
        public <K, V, W extends BoundedWindow> MultimapSideInputHandler<K, V, W> forSideInput(
            String pTransformId, String sideInputId, Coder<K> keyCoder, Coder<V> valueCoder,
            Coder<W> windowCoder) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  public interface BagUserStateHandler<K, V, W extends BoundedWindow> {
    Reiterable<V> get(K key, W window);
    void append(K key, Iterator<V> values);
    void clear(K key);
  }

  public interface BagUserStateHandlerFactory {
    <K, V, W extends BoundedWindow> BagUserStateHandler<K, V, W> forUserState(
        String pTransformId,
        String userStateId,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        Coder<W> windowCoder);

    static BagUserStateHandlerFactory unsupported() {
      return new BagUserStateHandlerFactory() {
        @Override
        public <K, V, W extends BoundedWindow> BagUserStateHandler<K, V, W> forUserState(
            String pTransformId, String userStateId, Coder<K> keyCoder, Coder<V> valueCoder,
            Coder<W> windowCoder) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
