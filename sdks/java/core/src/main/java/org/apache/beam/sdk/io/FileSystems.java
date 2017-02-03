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

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.CreateOptions.StandardCreateOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.ShardingWritableByteChannel;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/**
 * Clients facing {@link FileSystem} utility.
 */
public class FileSystems {

  // Pattern that matches shard placeholders within a shard template.
  private static final Pattern SHARD_FORMAT_RE = Pattern.compile("(S+|N+)");

  public static final String DEFAULT_SCHEME = "default";

  // Regex to validate the scheme.
  private static final Pattern URI_SCHEME_PATTERN = Pattern.compile("^[a-zA-Z][-a-zA-Z0-9+.]*$");

  // Regex to parse the scheme.
  private static final Pattern URI_SCHEME_PARSING_PATTERN =
      Pattern.compile("(?<scheme>[a-zA-Z][-a-zA-Z0-9+.]*)://.*");

  private static final Map<String, FileSystemRegistrar> SCHEME_TO_REGISTRAR =
      new ConcurrentHashMap<>();

  private static PipelineOptions defaultConfig;

  private static final Map<String, PipelineOptions> SCHEME_TO_DEFAULT_CONFIG =
      new ConcurrentHashMap<>();

  static {
    loadFileSystemRegistrars();
  }

  /********************************** METHODS FOR CLIENT **********************************/

  /**
   * Creates a write channel for the given filename.
   */
  public static WritableByteChannel create(ResourceId resourceId, String mimeType)
      throws IOException {
    return create(resourceId, StandardCreateOptions.builder().setMimeType(mimeType).build());
  }

  /**
   * Creates a write channel for the given filename with {@link CreateOptions}.
   */
  public static WritableByteChannel create(ResourceId resourceId, CreateOptions options) throws IOException {
    return getFileSystemInternal(resourceId).create(resourceId, options);
  }

  /**
   * Returns a read channel for the given file.
   *
   * <p>The file is not expanded; it is used verbatim.
   *
   * <p>If seeking is supported, then this returns a
   * {@link java.nio.channels.SeekableByteChannel}.
   */
  public static ReadableByteChannel open(ResourceId resourceId) throws IOException {
    return getFileSystemInternal(resourceId).open(resourceId);
  }

  /**
   * Copies a {@link List} of files from one location to another.
   *
   * <p>The number of source files must equal the number of destination files.
   * Destination files will be created recursively.
   *
   * @param srcResourceIds the source files.
   * @param destResourceIds the destination files.
   */
  public static void copy(
      List<ResourceId> srcResourceIds,
      List<ResourceId> destResourceIds) throws IOException {
    checkArgument(
        srcResourceIds.size() == destResourceIds.size(),
        "Number of source files %s must equal number of destination files %s",
        srcResourceIds.size(),
        destResourceIds.size());
    if (srcResourceIds.isEmpty() && destResourceIds.isEmpty()) {
      return;
    }
    getFileSystemInternal(srcResourceIds.get(0)).copy(srcResourceIds, destResourceIds);
  }

  /**
   * Renames a {@link List} of files from one location to another.
   *
   * <p>The number of source files must equal the number of destination files.
   * Destination files will be created recursively.
   *
   * @param srcResourceIds the source files.
   * @param destResourceIds the destination files.
   */
  public static void rename(
      List<ResourceId> srcResourceIds,
      List<String> destResourceIds) throws IOException {
    checkArgument(
        srcResourceIds.size() == destResourceIds.size(),
        "Number of source files %s must equal number of destination files %s",
        srcResourceIds.size(),
        destResourceIds.size());
    if (srcResourceIds.isEmpty() && destResourceIds.isEmpty()) {
      return;
    }
    getFileSystemInternal(srcResourceIds.get(0)).rename(srcResourceIds, destResourceIds);
  }

  /**
   * Deletes a collection of files (including directories).
   *
   * @param resourceIds the files to delete.
   */
  public static void delete(Collection<ResourceId> resourceIds) throws IOException {
    checkNotNull(resourceIds, "files");
    if (resourceIds.isEmpty()) {
      return;
    }
    getFileSystemInternal(resourceIds.iterator().next()).delete(resourceIds);
  }

  /********************************** METHODS FOR REGISTRATION **********************************/

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
   * Sets the default configuration in workers.
   *
   * <p>It will be used in {@link FileSystemRegistrar FileSystemRegistrars} for all schemes.
   */
  public static void setDefaultConfigInWorkers(PipelineOptions options) {
    defaultConfig = checkNotNull(options, "options");
  }

  /**
   * Internal method to get {@link FileSystem} for {@code spec}.
   */
  @VisibleForTesting
  static FileSystem getFileSystemInternal(ResourceId resourceId) {
    String lowerCaseScheme = resourceId.getScheme().toLowerCase();
    return getRegistrarInternal(lowerCaseScheme).fromOptions(defaultConfig);
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

  /********************************** METHODS FOR TESTING **********************************/

  /**
   * Sets the {@link FileSystemRegistrar} and overrides its scheme.
   */
  @VisibleForTesting
  public static void overrideFileSystemRegistrarsForTests(FileSystemRegistrar registrar) {
    SCHEME_TO_REGISTRAR.put(registrar.getScheme(), registrar);
  }
}
