/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.spark;

public final class TimestampedElement<T> {

  private final long timestamp;
  private final T el;

  public TimestampedElement(long timestamp, T el) {
    this.timestamp = timestamp;
    this.el = el;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public T getElement() {
    return el;
  }
}
