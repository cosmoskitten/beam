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

import com.google.common.base.Preconditions;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Naming template for generating file names for sharded IO.
 */
public class FileNameTemplate {
  /**
   * Default instance, using {@link ShardNameTemplate#INDEX_OF_MAX} and no prefix or suffix. Useful
   * to build off of.
   */
  public static final FileNameTemplate DEFAULT = FileNameTemplate.of(
      "", ShardNameTemplate.INDEX_OF_MAX, "");

  // Pattern that matches shard placeholders within a shard template.
  private static final Pattern SHARD_FORMAT_RE = Pattern.compile("(S+|N+)");

  private final String prefix;
  private final String shardTemplate;
  private final String suffix;

  /**
   * Construct a new template built from a prefix, shard template (with shard numbers
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
  public static FileNameTemplate of(String prefix, String shardTemplate, String suffix) {
    return new FileNameTemplate(prefix, shardTemplate, suffix);
  }

  /**
   * Clone the {@link FileNameTemplate} with the specified prefix.
   */
  public FileNameTemplate withPrefix(String prefix) {
    return FileNameTemplate.of(prefix, shardTemplate, suffix);
  }

  /**
   * Clone the {@link FileNameTemplate} with the specified shard name prefix.
   */
  public FileNameTemplate withShardTemplate(String shardTemplate) {
    return FileNameTemplate.of(prefix, shardTemplate, suffix);
  }

  /**
   * Clone the {@link FileNameTemplate} with the specified suffix.
   */
  public FileNameTemplate withSuffix(String suffix) {
    return FileNameTemplate.of(prefix, shardTemplate, suffix);
  }

  private FileNameTemplate(String prefix, String shardTemplate, String suffix) {
    this.prefix = checkNotNull(prefix);
    this.shardTemplate = checkNotNull(shardTemplate);
    this.suffix = checkNotNull(suffix);
  }

  String getPrefix() {
    return prefix;
  }

  String getShardTemplate() {
    return shardTemplate;
  }

  String getSuffix() {
    return suffix;
  }

  /**
   * Constructs a fully qualified name from components.
   *
   * @see FileNameTemplate#of(String, String, String)
   */
  public String apply(int shardNum, int numShards) {
    Preconditions.checkPositionIndex(shardNum, numShards);

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

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FileNameTemplate) {
      FileNameTemplate that = (FileNameTemplate) obj;
      return Objects.equals(this.prefix, that.prefix)
          && Objects.equals(this.shardTemplate, that.shardTemplate)
          && Objects.equals(this.suffix, that.suffix);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(prefix, shardTemplate, suffix);
  }

  @Override
  public String toString() {
    return String.format("%s%s%s", prefix, shardTemplate, suffix);
  }
}
