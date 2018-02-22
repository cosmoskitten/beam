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
package org.apache.beam.sdk.io.common;

import java.util.Date;
import java.util.Map;

/**
 * This class contains common helper methods to ease writing IO Tests.
 */
public class IOTestHelper {

  private IOTestHelper() {
  }

  public static String appendTimestampSuffix(String text) {
    return String.format("%s_%s", text, new Date().getTime());
  }

  public static String getHashForRecordCount(int recordCount, Map<Integer, String> hashes) {
    String hash = hashes.get(recordCount);
    if (hash == null) {
      throw new UnsupportedOperationException(
        String.format("No hash for that record count: %s", recordCount)
      );
    }
    return hash;
  }
}
