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

package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link PTransform} that converts a {@link PCollection} of type {@code T} to a
 * {@link PCollection} of type {@code String}.
 */
public class ToString<T> extends PTransform<PCollection<T>, PCollection<String>> {

  /**
   * Returns a {@link ToString} transform.
   *
   * @param <T> the type of the input {@code PCollection}.
   */
  public static <T> ToString<T> create() {
    return new ToString<>();
  }

  private ToString() {
  }

  @Override
  public PCollection<String> expand(PCollection<T> input) {
    return input.apply(MapElements.via(new ToStringFunction<T>()));
  }

  private static class ToStringFunction<T> extends SimpleFunction<T, String> {
    @Override
    public String apply(T input) {
      return input.toString();
    }
  }
}
