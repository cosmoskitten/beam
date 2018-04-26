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

package org.apache.beam.runners.samza.state;

import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.state.MapState;


/**
 * Samza's extended MapState.
 *
 * <p>NOTE: this is a temporary API until Beam allows the iterable/iterator created
 * from the state to be explicitly closed.
 */
@Experimental(Experimental.Kind.STATE)
public interface SamzaMapState<KeyT, ValueT> extends MapState<KeyT, ValueT> {

  /**
   * Returns an iterator from the current map state. It MUST be closed after use.
   * Note this is different from the iterable implementation in {@link MapState#entries()}},
   * where we load the entries into memory and return iterable from that. The reason
   * is because in entries() we are not able to track the iterator created from the store
   * and close it when it's not in use, so we need to load into memory and let Java GC
   * clean up the content. To manipulate large state that doesn't fit in memory, we also need
   * this method so it's possible to iterate on large data set and close the iterator
   * when not needed.
   *
   * @return a closeable iterator
   */
  CloseableIterator<Map.Entry<KeyT, ValueT>> iterator();
}
