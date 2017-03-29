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
package org.apache.beam.sdk.util.state;

/**
 * State containing no duplicate elements.
 * Items can be added to the set and the contents read out.
 *
 * @param <T> The type of elements in the set.
 */
public interface SetState<T> extends CombiningState<T, Iterable<T>> {

  /**
   * Returns true if this set contains the specified element.
   */
  ReadableState<Boolean> contains(T t);

  /**
   * Add a value to the buffer if it is not already present.
   * If this set already contains the element, the call leaves the set
   * unchanged and returns false.
   */
  ReadableState<Boolean> addIfAbsent(T t);

  /**
   * Removes the specified element from this set if it is present.
   */
  void remove(T t);

  @Override
  SetState<T> readLater();
}
