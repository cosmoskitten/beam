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
package org.apache.beam.sdk.nexmark;

import javax.annotation.Nullable;

/** Known "Nexmark" queries, some of which are of our own devising but use the same data set. */
@SuppressWarnings("ImmutableEnumChecker")
public enum NexmarkQueryName {
  // A baseline
  ZERO,

  // The actual Nexmark queries
  ONE,
  TWO,
  THREE,
  FOUR,
  FIVE,
  SIX,
  SEVEN,
  EIGHT,

  // Misc other queries against the data set
  NINE,
  TEN,
  ELEVEN,
  TWELVE,
  JOIN_TO_FILES;

  public static @Nullable NexmarkQueryName fromNumber(int nexmarkQueryNumber) {
    switch (nexmarkQueryNumber) {
      case 0:
        return ZERO;
      case 1:
        return ONE;
      case 2:
        return TWO;
      case 3:
        return THREE;
      case 4:
        return FOUR;
      case 5:
        return FIVE;
      case 6:
        return SIX;
      case 7:
        return SEVEN;
      case 8:
        return EIGHT;
      case 9:
        return NINE;
      case 10:
        return TEN;
      case 11:
        return ELEVEN;
      case 12:
        return TWELVE;
      default:
        return null;
    }
  }
}
