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
package org.apache.beam.sdk.io;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.commons.lang3.SystemUtils;

/**
 * Clients facing {@link FileSystem} utility.
 */
public class FileSystems {

  public static final String DEFAULT_SCHEME = "default";

  private static final Pattern URI_SCHEME_SYNTAX_PATTERN =
      Pattern.compile("^[a-zA-Z][-a-zA-Z0-9+.]*$");

  // Regex to parse the scheme.
  private static final Pattern URI_SCHEME_PARSING_PATTERN =
      Pattern.compile("(?<scheme>[a-zA-Z][-a-zA-Z0-9+.]*)://.*");

  // Regex to parse the URI (defined in https://tools.ietf.org/html/rfc3986#appendix-B).
  private static final Pattern URI_PARSING_PATTERN = Pattern.compile(
      "^((?<scheme>[^:/?#]+):)?(//(?<authority>[^/?#]*))?(?<path>[^?#]*)"
          + "(\\?(?<query>[^#]*))?(#(?<fragment>.*))?");

  private static final Map<String, FileSystemRegistrar> SCHEME_TO_REGISTRAR =
      new ConcurrentHashMap<>();

  private static final Map<String, PipelineOptions> SCHEME_TO_DEFAULT_CONFIG =
      new ConcurrentHashMap<>();

  static {
    loadFileSystemRegistrars();
  }

  /**
   * Loads available {@link FileSystemRegistrar} services.
   */
  private static void loadFileSystemRegistrars() {
    SCHEME_TO_REGISTRAR.clear();
    Set<FileSystemRegistrar> registrars =
        Sets.newTreeSet(ReflectHelpers.ObjectsClassComparator.INSTANCE);
    registrars.addAll(Lists.newArrayList(
        ServiceLoader.load(FileSystemRegistrar.class, ReflectHelpers.findClassLoader())));

    verifySchemesAreUnique(registrars);

    for (FileSystemRegistrar registrar : registrars) {
      SCHEME_TO_REGISTRAR.put(registrar.getScheme().toLowerCase(), registrar);
    }
  }

  /**
   * Converts the user spec string to URI.
   *
   * <p>It uses {@link Paths} to handle non-URI Windows OS paths.
   */
  public static URI specToURI(String spec) {
    try {
      // Using the URI constructor in order to escape special characters, such as spaces.
      // URI.create() will throw for illegal characters if it is called directly.
      Matcher matcher = URI_PARSING_PATTERN.matcher(spec);
      checkState(matcher.matches(), "spec: %s doesn't match URI regex.", spec);
      return new URI(
          matcher.group("scheme"),
          matcher.group("authority"),
          matcher.group("path"),
          matcher.group("query"),
          matcher.group("fragment"));
    } catch (URISyntaxException e) {
      // Try convert non-URI Windows OS spec to Path, and then to URI.
      String lowerCaseScheme = getLowerCaseScheme(spec);
      if (lowerCaseScheme.equals(LocalFileSystemRegistrar.LOCAL_FILE_SCHEME)
          && SystemUtils.IS_OS_WINDOWS) {
        // Replace asterisks in the spec with the placeholder (uuid) before converting to a Path.
        String uuid = UUID.randomUUID().toString();
        String specAsterisksReplaced = spec.replaceAll("\\*", uuid);
        String uriAsterisksReplaced = Paths.get(specAsterisksReplaced).toUri().toString();
        return URI.create(uriAsterisksReplaced.replaceAll(uuid, "\\*"));
      } else {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }
  }

  /**
   * Sets the default configuration to be used with a {@link FileSystemRegistrar} for the provided
   * {@code scheme}.
   *
   * <p>Syntax: <pre>scheme = alpha *( alpha | digit | "+" | "-" | "." )</pre>
   * Upper case letters are treated as the same as lower case letters.
   */
  public static void setDefaultConfig(String scheme, PipelineOptions options) {
    String lowerCaseScheme = checkNotNull(scheme, "scheme").toLowerCase();
    checkArgument(
        URI_SCHEME_SYNTAX_PATTERN.matcher(lowerCaseScheme).matches(),
        String.format("Scheme: [%s] doesn't match URI syntax: %s",
            lowerCaseScheme, URI_SCHEME_SYNTAX_PATTERN.pattern()));
    checkArgument(
        SCHEME_TO_REGISTRAR.containsKey(lowerCaseScheme),
        String.format("No FileSystemRegistrar found for scheme: [%s].", lowerCaseScheme));
    SCHEME_TO_DEFAULT_CONFIG.put(lowerCaseScheme, checkNotNull(options, "options"));
  }

  // TODO: change to isWindowsOSPath()
  @VisibleForTesting
  static String getLowerCaseScheme(String spec) {
    Matcher matcher = URI_SCHEME_PARSING_PATTERN.matcher(spec);
    if (!matcher.matches()) {
      return LocalFileSystemRegistrar.LOCAL_FILE_SCHEME;
    }
    String scheme = matcher.group("scheme");
    if (Strings.isNullOrEmpty(scheme)) {
      return LocalFileSystemRegistrar.LOCAL_FILE_SCHEME;
    } else {
      return scheme.toLowerCase();
    }
  }

  @VisibleForTesting
  static PipelineOptions getDefaultConfig(String scheme) {
    return SCHEME_TO_DEFAULT_CONFIG.get(scheme.toLowerCase());
  }

  /**
   * Internal method to get {@link FileSystem} for {@code spec}.
   */
  @VisibleForTesting
  static FileSystem getFileSystemInternal(URI uri) {
    String lowerCaseScheme = (uri.getScheme() != null
        ? uri.getScheme().toLowerCase() : LocalFileSystemRegistrar.LOCAL_FILE_SCHEME);
    return getRegistrarInternal(lowerCaseScheme).fromOptions(getDefaultConfig(lowerCaseScheme));
  }

  /**
   * Internal method to get {@link FileSystemRegistrar} for {@code scheme}.
   */
  @VisibleForTesting
  static FileSystemRegistrar getRegistrarInternal(String scheme) {
    String lowerCaseScheme = scheme.toLowerCase();
    if (SCHEME_TO_REGISTRAR.containsKey(lowerCaseScheme)) {
      return SCHEME_TO_REGISTRAR.get(lowerCaseScheme);
    } else if (SCHEME_TO_REGISTRAR.containsKey(DEFAULT_SCHEME)) {
      return SCHEME_TO_REGISTRAR.get(DEFAULT_SCHEME);
    } else {
      throw new IllegalStateException("Unable to find registrar for " + scheme);
    }
  }

  @VisibleForTesting
  static void verifySchemesAreUnique(Set<FileSystemRegistrar> registrars) {
    Multimap<String, FileSystemRegistrar> registrarsBySchemes =
        TreeMultimap.create(Ordering.<String>natural(), Ordering.arbitrary());

    for (FileSystemRegistrar registrar : registrars) {
      registrarsBySchemes.put(registrar.getScheme().toLowerCase(), registrar);
    }
    for (Entry<String, Collection<FileSystemRegistrar>> entry
        : registrarsBySchemes.asMap().entrySet()) {
      if (entry.getValue().size() > 1) {
        String conflictingRegistrars = Joiner.on(", ").join(
            FluentIterable.from(entry.getValue())
                .transform(new Function<FileSystemRegistrar, String>() {
                  @Override
                  public String apply(@Nonnull FileSystemRegistrar input) {
                    return input.getClass().getName();
                  }})
                .toSortedList(Ordering.<String>natural()));
        throw new IllegalStateException(String.format(
            "Scheme: [%s] has conflicting registrars: [%s]",
            entry.getKey(),
            conflictingRegistrars));
      }
    }
  }
}
