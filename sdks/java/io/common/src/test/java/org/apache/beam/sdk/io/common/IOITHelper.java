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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Methods common to all types of IOITs.
 */
public class IOITHelper {
  private static final Logger LOG = LoggerFactory.getLogger(IOITHelper.class);

  private IOITHelper() {
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

  public static void retry(RetryFunction function, int attempts, long delay)
      throws InterruptedException {
    ArrayList<Exception> errorList = new ArrayList<Exception>();
    int countAttempts = 0;

    while (countAttempts < attempts) {
      try {
        function.run();
        return;
      } catch (Exception e) {
        LOG.warn(
            "Attempt #{} of {} threw exception: {}", countAttempts + 1, attempts, e.getMessage());
        errorList.add(e);
        countAttempts++;
        if (countAttempts == attempts) {
          throw RetryException.composeErrors(errorList);
        } else {
          Thread.sleep(delay);
        }
      }
    }
  }

  static class RetryException extends RuntimeException {
    public RetryException(String message) {
      super(message);
    }

    public static RetryException composeErrors(ArrayList<Exception> errors) {
      StringBuilder errorMessage = new StringBuilder();
      for (int i = 0; i < errors.size(); i++) {
        errorMessage.append('\n');
        errorMessage
            .append("Attempt #")
            .append(i + 1)
            .append(" of ")
            .append(errors.size())
            .append(" threw exception:");
        errorMessage.append(stackTraceAsString(errors.get(i)));
      }

      return new RetryException(errorMessage.toString());
    }

    private static String stackTraceAsString(Throwable t) {
      final StringWriter errors = new StringWriter();
      t.printStackTrace(new PrintWriter(errors));
      return errors.toString();
    }
  }
}
