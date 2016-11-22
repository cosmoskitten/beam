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
package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;

/**
 * Utility class for handling {@link URI}.
 */
public class PathUtils {

  private static final String URI_DELIMITER = "/";

  /**
   * Resolve multiple {@code others} against the {@code directory} sequentially.
   *
   * <p>Empty paths in {@code others} are ignored. If {@code others} contains one or more
   * absolute paths, then this method returns a path that starts with the last absolute path
   * in {@code others} joined with the remaining paths. Resolution of paths is highly
   * implementation dependent and therefore unspecified.
   */
  public static String resolve(String directory, String... others) {
    URI dirUri;
    if (directory.endsWith(URI_DELIMITER)) {
      dirUri = URI.create(directory);
    } else {
      dirUri = URI.create(directory + URI_DELIMITER);
    }
    URI fullPath = dirUri;

    for (String other : others) {
      fullPath = fullPath.resolve(other);
    }
    return fullPath.toString();
  }

  /**
   * Returns the name of the file or directory denoted by this path as a
   * {@code String}. The file name is the <em>farthest</em> element from
   * the root in the directory hierarchy.
   *
   * @return a string representing the name of the file or directory,
   *         or an empty {@code String} if this path is "/" or empty,
   *         or {@code null} if this path is {@code null}.
   */
  public static String getFileName(String path) {
    checkNotNull(path, "path");
    if (path.isEmpty() || path.equals(URI_DELIMITER)) {
      return "";
    } else if (path.endsWith(URI_DELIMITER)) {
      return path.substring(
          path.lastIndexOf(URI_DELIMITER, path.length() - 2) + 1,
          path.length() - 1);
    } else {
      return path.substring(path.lastIndexOf(URI_DELIMITER) + 1);
    }
  }
}
