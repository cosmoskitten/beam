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
      ret = resolveAgainstDirectory(ret.normalize(), URI.create(other));
    }
    return ret.toString();
  }

  private static URI resolveAgainstDirectory(@Nonnull URI base, @Nonnull URI other) {
    checkNotNull(base, "directory");
    checkNotNull(other, "other");
    checkArgument(!other.toString().isEmpty(), "Expected other is not empty.");
    checkArgument(
        Strings.isNullOrEmpty(other.getQuery()),
        String.format("Expected no query in other: [%s].", other));
    checkArgument(
        Strings.isNullOrEmpty(other.getFragment()),
        String.format("Expected no fragment in other: [%s].", other));

    String path;
    if (base.getPath().isEmpty() || base.getPath().endsWith(URI_DELIMITER)) {
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
}
