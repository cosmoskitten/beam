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
package org.apache.beam.runners.fnexecution;

import java.util.function.Predicate;

/** Utilities for {@link org.apache.beam.model.pipeline.v1.RunnerApi.Components}. */
public class Componentses {
  // TODO: Remove this class and replace with SyntheticNodes.uniqueId once
  // https://github.com/apache/beam/pull/4977 lands.

  /**
   * Creates a unique id for the given pattern.
   * @param idPrefix the prefix for the new component id
   * @param idUsed a predicate that indicates whether the given id is already used
   */
  public static String uniquifyId(String idPrefix, Predicate<String> idUsed) {
    if (!idUsed.test(idPrefix)) {
      return idPrefix;
    }
    int i = 0;
    while (idUsed.test(String.format("%s_%s", idPrefix, i))) {
      i++;
    }
    return String.format("%s_%s", idPrefix, i);
  }
}
