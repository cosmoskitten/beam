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
  PASSTHROUGH,

  // The actual Nexmark queries
  CURRENCY_CONVERSION, // Query 1
  SELECTION, // Query 2
  LOCAL_ITEM_SUGGESTION, // Query 3
  AVERAGE_PRICE_FOR_CATEGORY, // Query 4
  HOT_ITEMS, // Query 5
  AVERAGE_SELLING_PRICE_BY_SELLER, // Query 6
  HIGHEST_BID, // Query 7
  MONITOR_NEW_USERS, // Query 8

  // Misc other queries against the data set
  WINNING_BIDS, // Query "9"
  LOG_TO_SHARDED_FILES, // Query "10"
  USER_SESSIONS, // Query "11"
  PROCESSING_TIME_WINDOWS; // Query "12"

  public static @Nullable NexmarkQueryName fromNumber(int nexmarkQueryNumber) {
    switch (nexmarkQueryNumber) {
      case 0:
        return PASSTHROUGH;

        // The actual Nexmark queries
      case 1:
        return CURRENCY_CONVERSION;
      case 2:
        return SELECTION;
      case 3:
        return LOCAL_ITEM_SUGGESTION;
      case 4:
        return AVERAGE_PRICE_FOR_CATEGORY;
      case 5:
        return HOT_ITEMS;
      case 6:
        return AVERAGE_SELLING_PRICE_BY_SELLER;
      case 7:
        return HIGHEST_BID;
      case 8:
        return MONITOR_NEW_USERS;

        // Extras we made up
      case 9:
        return WINNING_BIDS;
      case 10:
        return LOG_TO_SHARDED_FILES;
      case 11:
        return USER_SESSIONS;
      case 12:
        return PROCESSING_TIME_WINDOWS;
      default:
        return null;
    }
  }
}
