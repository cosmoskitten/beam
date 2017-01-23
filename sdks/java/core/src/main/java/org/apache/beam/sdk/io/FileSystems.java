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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

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
  public static WritableByteChannel create(String filename, String mimeType)
      throws IOException {
    return create(filename, CreateOptions.builder().setMimeType(mimeType).build());
  }

  /**
   * Creates a write channel for the given file components.
   *
   * <p>If numShards is specified, then a ShardingWritableByteChannel is
   * returned.
   *
   * <p>Shard numbers are 0 based, meaning they start with 0 and end at the
   * number of shards - 1.
   *
   * <p>TODO: reduce the number of parameters by providing a ShardTemplate class.
   */
  public static WritableByteChannel create(String prefix, String shardTemplate,
      String suffix, int numShards, String mimeType) throws IOException {
    if (numShards == 1) {
      return create(constructName(prefix, shardTemplate, suffix, 0, 1),
          mimeType);
    }

    // It is the callers responsibility to close this channel.
    @SuppressWarnings("resource")
    ShardingWritableByteChannel shardingChannel =
        new ShardingWritableByteChannel();

    Set<String> outputNames = new HashSet<>();
    for (int i = 0; i < numShards; i++) {
      String outputName =
          constructName(prefix, shardTemplate, suffix, i, numShards);
      if (!outputNames.add(outputName)) {
        throw new IllegalArgumentException(
            "Shard name collision detected for: " + outputName);
      }
      WritableByteChannel channel = create(outputName, mimeType);
      shardingChannel.addChannel(channel);
    }

    return shardingChannel;
  }

  private static WritableByteChannel create(String path, CreateOptions options) throws IOException {
    return getFileSystemInternal(path).create(path, options);
  }

  /**
   * Returns a read channel for the given file.
   *
   * <p>The file is not expanded; it is used verbatim.
   *
   * <p>If seeking is supported, then this returns a
   * {@link java.nio.channels.SeekableByteChannel}.
   */
  public static ReadableByteChannel open(String path) throws IOException {
    return getFileSystemInternal(path).open(path);
  }

  /**
   * Copies a {@link List} of files from one location to another.
   *
   * <p>The number of source files must equal the number of destination files.
   * Destination files will be created recursively.
   *
   * @param srcFiles the source files.
   * @param destFiles the destination files.
   */
  public static void copy(List<String> srcFiles, List<String> destFiles) throws IOException {
    checkArgument(
        srcFiles.size() == destFiles.size(),
        "Number of source files %s must equal number of destination files %s",
        srcFiles.size(),
        destFiles.size());
    if (srcFiles.isEmpty() && destFiles.isEmpty()) {
      return;
    }
    getFileSystemInternal(srcFiles.get(0)).copy(srcFiles, destFiles);
  }

  /**
   * Renames a {@link List} of files from one location to another.
   *
   * <p>The number of source files must equal the number of destination files.
   * Destination files will be created recursively.
   *
   * @param srcFiles the source files.
   * @param destFiles the destination files.
   */
  public static void rename(List<String> srcFiles, List<String> destFiles) throws IOException {
    checkArgument(
        srcFiles.size() == destFiles.size(),
        "Number of source files %s must equal number of destination files %s",
        srcFiles.size(),
        destFiles.size());
    if (srcFiles.isEmpty() && destFiles.isEmpty()) {
      return;
    }
    getFileSystemInternal(srcFiles.get(0)).rename(srcFiles, destFiles);
  }

  /**
   * Deletes a collection of files (including directories).
   *
   * @param files the files to delete.
   */
  public static void delete(Collection<String> files) throws IOException {
    checkNotNull(files, "files");
    if (files.isEmpty()) {
      return;
    }
    getFileSystemInternal(files.iterator().next()).delete(files);
  }

  /**
   * Constructs a fully qualified name from components.
   *
   * <p>The name is built from a prefix, shard template (with shard numbers
   * applied), and a suffix.  All components are required, but may be empty
   * strings.
   *
   * <p>Within a shard template, repeating sequences of the letters "S" or "N"
   * are replaced with the shard number, or number of shards respectively.  The
   * numbers are formatted with leading zeros to match the length of the
   * repeated sequence of letters.
   *
   * <p>For example, if prefix = "output", shardTemplate = "-SSS-of-NNN", and
   * suffix = ".txt", with shardNum = 1 and numShards = 100, the following is
   * produced:  "output-001-of-100.txt".
   */
  public static String constructName(String prefix,
      String shardTemplate, String suffix, int shardNum, int numShards) {
    // Matcher API works with StringBuffer, rather than StringBuilder.
    StringBuffer sb = new StringBuffer();
    sb.append(prefix);

    Matcher m = SHARD_FORMAT_RE.matcher(shardTemplate);
    while (m.find()) {
      boolean isShardNum = (m.group(1).charAt(0) == 'S');

      char[] zeros = new char[m.end() - m.start()];
      Arrays.fill(zeros, '0');
      DecimalFormat df = new DecimalFormat(String.valueOf(zeros));
      String formatted = df.format(isShardNum
                                   ? shardNum
                                   : numShards);
      m.appendReplacement(sb, formatted);
    }
    m.appendTail(sb);

    sb.append(suffix);
    return sb.toString();
  }

  /**
   * A class that configures how to create files.
   */
  @AutoValue
  public abstract static class CreateOptions {

    /**
     * The file mime type.
     */
    public abstract String mimeType();

    public static Builder builder() {
      return new AutoValue_FileSystems_CreateOptions.Builder();
    }

    /**
     * Builder for {@link CreateOptions}.
     */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setMimeType(String value);

      public abstract CreateOptions build();
    }
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

  /**
   * Internal method to get {@link FileSystem} for {@code spec}.
   */
  @VisibleForTesting
  static FileSystem getFileSystemInternal(String spec) {
      checkState(
          defaultConfig != null,
          "Expect the runner have called setDefaultConfigInWorkers().");
      String lowerCaseScheme = getLowerCaseScheme(spec);
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

  /**
   * An abstract {@link FileSystem} whose methods are public and could be mocked.
   */
  public abstract static class MockFileSystem extends FileSystem {
    @Override
    public abstract WritableByteChannel create(String uri, FileSystems.CreateOptions options)
        throws IOException;
    @Override
    public abstract ReadableByteChannel open(String uri) throws IOException;

    @Override
    public abstract void rename(List<String> srcUris, List<String> destUris) throws IOException;

    @Override
    public abstract void delete(Collection<String> uris) throws IOException;
  }
}
