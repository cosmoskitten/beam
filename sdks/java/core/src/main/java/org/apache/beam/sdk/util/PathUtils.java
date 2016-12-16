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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import java.net.URI;

/**
 * Utility class for handling file paths.
 */
public class PathUtils {

  private static final String URI_DELIMITER = "/";

  /**
   * Resolve multiple {@code others} against the {@code directory} sequentially.
   *
   * <p>Unlike {@link URI#resolve}, {@link #resolveAgainstDirectory} includes the last segment of
   * the path. Other rules of {@link URI#resolve} apply the same. For example, ".", ".." are
   * normalized. Sees {@link URI#resolve} and {@link URI#normalize} for details.
   *
   * <p>{@code others} should not be empty, and should be valid {@link URI URIs} strings but without
   * query and fragment components.
   */
  public static String resolveAgainstDirectory(@Nonnull String base, @Nonnull String... others) {
    URI ret = URI.create(base);
    for (String other : others) {
      ret = resolveAgainstDirectory(ret, URI.create(other));
    }
    return ret.toString();
  }

  private static URI resolveAgainstDirectory(@Nonnull URI base, @Nonnull URI other) {
    checkNotNull(base, "directory");
    checkNotNull(other, "other");
    checkArgument(!other.toString().isEmpty(), "Expected other is not empty.");
    checkArgument(
        other.getQuery().isEmpty(),
        String.format("Expected no query in other: [%s].", other));
    checkArgument(
        other.getFragment().isEmpty(),
        String.format("Expected no fragment in other: [%s].", other));

    String path;
    if (base.getPath().endsWith(URI_DELIMITER)) {
      path = base.getPath();
    } else {
      path = base.getPath() + URI_DELIMITER;
    }
    try {
      return new URI(
          base.getScheme(),
          base.getAuthority(),
          path,
          base.getQuery(),
          base.getFragment()).resolve(other);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to replace the path component in URI: [%s] with [%s].",
              base.toString(), path),
          e);
    }
  }

  /**
   * Returns the directory portion of the {@code path}, which includes all but the file name.
   *
   * @return a string representing the directory portion of the {@code path},
   *         or the {@code path} itself, if it is already a directory,
   *         or an empty {@link String}, if {@code path} is "~", ".", "..".
   */
  public static String getDirectory(@Nonnull String path) {
    checkNotNull(path, "path");
    checkArgument(!path.isEmpty(), "Expected path is not empty.");

    URI pathUri = URI.create(path);
    checkArgument(
        pathUri.getQuery().isEmpty(),
        String.format("Expected no query in path: [%s].", path));
    checkArgument(
        pathUri.getFragment().isEmpty(),
        String.format("Expected no fragment in path: [%s].", path));

    return pathUri.resolve("").toString();
  }

  /**
   * Returns the name of the file or directory denoted by this path as a
   * {@code String}. The file name is the <em>farthest</em> element from
   * the root in the directory hierarchy.
   *
   * @return a string representing the name of the file or directory,
   *         or the {@code path} itself, if {@code path} is "~", ".", "..",
   *         or an empty {@link String} if this path is "/" or empty.
   */
  public static String getFileName(@Nonnull String path) {
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
