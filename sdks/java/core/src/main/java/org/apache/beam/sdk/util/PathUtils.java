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

import com.google.api.client.repackaged.com.google.common.base.Strings;

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
   * Resolve multiple {@code others} against the given {@code directory} sequentially.
   *
   * @see {@link #resolveAgainstDirectory(String, String)} for the differences
   * with {@link URI#resolve}.
   *
   * @throws IllegalArgumentException if others contains {@link URI} query or fragment components.
   */
  public static String resolveAgainstDirectory(
      @Nonnull String directory, @Nonnull String other, @Nonnull String... others) {
    String ret = resolveAgainstDirectory(directory, other);
    for (String str : others) {
      ret = resolveAgainstDirectory(ret, str);
    }
    return ret;
  }

  /**
   * Resolve {@code other} against the given {@code directory}.
   *
   * <p>Unlike {@link URI#resolve}, this function includes the last segment of the path component
   * in {@code directory}. For example, {@code PathUtils.resolveAgainstDirectory("/home", "output")}
   * returns "/home/output".
   *
   * <p>Other rules of {@link URI#resolve} apply the same. For example, ".", ".." are
   * normalized. Sees {@link URI#resolve} and {@link URI#normalize} for details.
   *
   * @throws IllegalArgumentException if other is empty, or is invalid {@link URI},
   * or contains {@link URI} query or fragment components.
   */
  public static String resolveAgainstDirectory(@Nonnull String directory, @Nonnull String other) {
    return resolveAgainstDirectory(URI.create(directory), URI.create(other)).toString();
  }

  private static URI resolveAgainstDirectory(@Nonnull URI directory, @Nonnull URI other) {
    checkNotNull(directory, "directory");
    checkNotNull(other, "other");
    checkArgument(!other.toString().isEmpty(), "Expected other is not empty.");
    checkArgument(
        Strings.isNullOrEmpty(other.getQuery()),
        String.format("Expected no query in other: [%s].", other));
    checkArgument(
        Strings.isNullOrEmpty(other.getFragment()),
        String.format("Expected no fragment in other: [%s].", other));

    String path = directory.getPath();
    if (!Strings.isNullOrEmpty(directory.getAuthority()) && Strings.isNullOrEmpty(path)) {
      // Workaround of [BEAM-1174]: path needs to be absolute if the authority exists.
      path = URI_DELIMITER;
    } else if (!Strings.isNullOrEmpty(path) && !path.endsWith(URI_DELIMITER)) {
      path = path + URI_DELIMITER;
    }
    try {
      return new URI(
          directory.getScheme(),
          directory.getAuthority(),
          path,
          directory.getQuery(),
          directory.getFragment()).resolve(other);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to replace the path component in URI: [%s] with [%s].",
              directory.toString(), path),
          e);
    }
  }
}
