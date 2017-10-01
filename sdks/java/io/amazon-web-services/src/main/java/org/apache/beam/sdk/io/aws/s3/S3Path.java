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
package org.apache.beam.sdk.io.aws.s3;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Implements the Java NIO {@link Path} API for AWS S3 paths. */
public class S3Path implements Path, Serializable {

  public static final String SCHEME = "s3";

  /** Matches a glob containing a wildcard, capturing the portion before the first wildcard. */
  private static final Pattern GLOB_PREFIX = Pattern.compile("(?<PREFIX>[^\\[*?]*)[\\[*?].*");

  public static S3Path fromUri(URI uri) {
    checkArgument(uri.getScheme().equals(SCHEME), "URI: %s is not an S3 URI", uri);
    checkArgument(uri.getPort() == -1, "S3 URI may not specify port: %s (%i)", uri, uri.getPort());
    checkArgument(
        Strings.isNullOrEmpty(uri.getUserInfo()),
        "S3 URI may not specify userInfo: %s (%s)",
        uri,
        uri.getUserInfo());
    checkArgument(
        Strings.isNullOrEmpty(uri.getQuery()),
        "S3 URI may not specify query: %s (%s)",
        uri,
        uri.getQuery());
    checkArgument(
        Strings.isNullOrEmpty(uri.getFragment()),
        "S3 URI may not specify fragment: %s (%s)",
        uri,
        uri.getFragment());

    String bucket = uri.getHost();
    String key = uri.getPath();
    if (key.startsWith("/")) {
      key = key.substring(1);
    }
    return fromComponents(bucket, key);
  }

  private static final Pattern S3_URI =
      Pattern.compile("(?<SCHEME>[^:]+)://(?<BUCKET>[^/]+)(/(?<OBJECT>.*))?");

  public static S3Path fromUri(String uri) {
    Matcher m = S3_URI.matcher(uri);
    checkArgument(m.matches(), "Invalid S3 URI: %s", uri);

    checkArgument(m.group("SCHEME").equalsIgnoreCase(SCHEME), "URI: %s is not an S3 URI", uri);
    return new S3Path(null, m.group("BUCKET"), m.group("OBJECT"));
  }

  public static S3Path fromComponents(String bucket, String key) {
    return new S3Path(null, bucket, key);
  }

  @Nullable private transient FileSystem fileSystem;
  @Nonnull private final String bucket;
  @Nonnull private final String key;

  private S3Path(FileSystem fileSystem, String bucket, String key) {
    // Bucket name rules:
    // http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
    // - Bucket name length must be [3~63] characters.
    // - "Labels" are defined as bucket name parts separated by single period delimiter.
    // - Each label starts and ends with lowercase letter or number.
    // - Bucket name cannot be formatted as an IP address.
    if (bucket == null) {
      bucket = "";
    }
    if (!bucket.isEmpty()) {
      checkArgument(
          3 <= bucket.length() && bucket.length() <= 63,
          "S3 bucket name length must be 3 to 63 characters");
      // Is this a valid IPv4 address?
      try {
        checkArgument(
            !(bucket.matches("[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}")
                && bucket.equals(Inet4Address.getByName(bucket).getHostAddress())),
            "S3 bucket name cannot be formatted as an IP address");
      } catch (UnknownHostException e) {
        // Expected; fall through.
      }

      String[] labels = bucket.split("\\.");
      for (String label : labels) {
        checkArgument(
            !label.isEmpty(),
            "S3 bucket name cannot start or end with a period, or have two consecutive periods");
        checkArgument(
            label.matches("[a-z0-9][-a-z0-9]+[a-z0-9]"),
            "S3 bucket name may contain lowercase letters, numbers, and hyphens. "
                + "Labels may not start or end with hyphen. 's3://%s/%s'",
            bucket,
            key);
      }
    }

    // Key name rules:
    // http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
    // - Length must be [1~1024] UTF-8 bytes.
    // - No character is explicitly prohibited (some are suggested to avoid).
    if (key == null) {
      key = "";
    }
    checkArgument(
        Charsets.UTF_8.encode(key).limit() <= 1024,
        "S3 key length must be <= 1024 bytes when UTF-8 encoded");

    this.fileSystem = fileSystem;
    this.bucket = Strings.nullToEmpty(bucket);
    this.key = Strings.nullToEmpty(key);
  }

  @Override
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public void setFileSystem(FileSystem fileSystem) {
    this.fileSystem = fileSystem;
  }

  public String getBucket() {
    return bucket;
  }

  public String getKey() {
    return key;
  }

  @Override
  public boolean isAbsolute() {
    return !bucket.isEmpty() || key.isEmpty();
  }

  public boolean isWildcard() {
    return GLOB_PREFIX.matcher(this.getKey()).matches();
  }

  @Override
  public S3Path getRoot() {
    return new S3Path(fileSystem, "", "");
  }

  @Override
  public S3Path getFileName() {
    int nameCount = getNameCount();
    if (nameCount < 2) {
      throw new UnsupportedOperationException(
          "Can't get filename from root path in the bucket: " + this);
    }
    return getName(nameCount - 1);
  }

  @Override
  public S3Path getParent() {
    if (bucket.isEmpty() && key.isEmpty()) {
      // The root path has no parent, by definition.
      return null;
    }

    if (key.isEmpty()) {
      // An S3 bucket. All buckets come from a common root.
      return getRoot();
    }

    // Skip last character, in case it is a trailing slash.
    int i = key.lastIndexOf('/', key.length() - 2);
    if (i <= 0) {
      if (bucket.isEmpty()) {
        // Relative paths are not attached to the root node.
        return null;
      }
      return new S3Path(fileSystem, bucket, "");
    }

    // Retain trailing slash.
    return new S3Path(fileSystem, bucket, key.substring(0, i + 1));
  }

  @Override
  public int getNameCount() {
    int count = bucket.isEmpty() ? 0 : 1;
    if (key.isEmpty()) {
      return count;
    }

    // Add another for each separator found.
    int index = -1;
    while ((index = key.indexOf('/', index + 1)) != -1) {
      count++;
    }

    return key.endsWith("/") ? count : count + 1;
  }

  @Override
  public S3Path getName(int index) {
    checkArgument(index >= 0);

    Iterator<Path> iterator = iterator();
    for (int i = 0; i < index; ++i) {
      checkArgument(iterator.hasNext());
      iterator.next();
    }

    checkArgument(iterator.hasNext());
    return (S3Path) iterator.next();
  }

  @Override
  public S3Path subpath(int beginIndex, int endIndex) {
    checkArgument(beginIndex >= 0);
    checkArgument(endIndex > beginIndex);

    Iterator<Path> iterator = iterator();
    for (int i = 0; i < beginIndex; ++i) {
      checkArgument(iterator.hasNext());
      iterator.next();
    }

    S3Path path = null;
    while (beginIndex < endIndex) {
      checkArgument(iterator.hasNext());
      if (path == null) {
        path = (S3Path) iterator.next();
      } else {
        path = path.resolve(iterator.next());
      }
      ++beginIndex;
    }

    return path;
  }

  @Override
  public boolean startsWith(Path other) {
    if (other instanceof S3Path) {
      S3Path otherS3Path = (S3Path) other;
      return startsWith(otherS3Path.bucketAndKey());
    }
    return startsWith(other.toString());
  }

  @Override
  public boolean startsWith(String prefix) {
    return bucketAndKey().startsWith(prefix);
  }

  @Override
  public boolean endsWith(Path other) {
    if (other instanceof S3Path) {
      S3Path otherS3Path = (S3Path) other;
      return endsWith(otherS3Path.bucketAndKey());
    }
    return endsWith(other.toString());
  }

  @Override
  public boolean endsWith(String suffix) {
    return bucketAndKey().endsWith(suffix);
  }

  @Override
  public S3Path normalize() {
    return this;
  }

  @Override
  public S3Path resolve(Path other) {
    if (other instanceof S3Path) {
      S3Path otherS3Path = (S3Path) other;
      if (otherS3Path.isAbsolute()) {
        return otherS3Path;
      }
      return resolve(otherS3Path.getKey());
    }
    return resolve(other.toString());
  }

  @Override
  public S3Path resolve(String other) {
    if (bucket.isEmpty() && key.isEmpty()) {
      // Resolve on a root path is equivalent to looking up a bucket and object.
      other = SCHEME + "://" + other;
    }

    if (other.startsWith(SCHEME + "://")) {
      S3Path otherS3Path = fromUri(other);
      otherS3Path.setFileSystem(getFileSystem());
      return otherS3Path;
    }

    if (other.isEmpty()) {
      // An empty component MUST refer to a directory.
      other = "/";
    }

    if (key.isEmpty()) {
      return new S3Path(fileSystem, bucket, other);
    } else if (key.endsWith("/")) {
      return new S3Path(fileSystem, bucket, key + other);
    } else {
      return new S3Path(fileSystem, bucket, key + "/" + other);
    }
  }

  @Override
  public S3Path resolveSibling(Path other) {
    throw new UnsupportedOperationException();
  }

  @Override
  public S3Path resolveSibling(String other) {
    if (getNameCount() < 2) {
      throw new UnsupportedOperationException("Can't resolve the sibling of a root path: " + this);
    }
    S3Path parent = getParent();
    return (parent == null) ? fromUri(other) : parent.resolve(other);
  }

  @Override
  public S3Path relativize(Path other) {
    throw new UnsupportedOperationException();
  }

  @Override
  public URI toUri() {
    try {
      return new URI(SCHEME, "//" + bucketAndKey(), null);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Unable to create URI for S3 path " + this);
    }
  }

  @Override
  public S3Path toAbsolutePath() {
    return this;
  }

  @Override
  public S3Path toRealPath(LinkOption... options) throws IOException {
    return this;
  }

  @Override
  public File toFile() {
    throw new UnsupportedOperationException();
  }

  @Override
  public WatchKey register(WatchService watcher, Kind<?>[] events, Modifier... modifiers)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public WatchKey register(WatchService watcher, Kind<?>... events) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<Path> iterator() {
    return new NameIterator(fileSystem, !bucket.isEmpty(), bucketAndKey());
  }

  private static class NameIterator implements Iterator<Path> {

    private final FileSystem innerFileSystem;
    private boolean fullPath;
    private String name;

    NameIterator(FileSystem innerFileSystem, boolean fullPath, String name) {
      this.innerFileSystem = innerFileSystem;
      this.fullPath = fullPath;
      this.name = name;
    }

    @Override
    public boolean hasNext() {
      return !Strings.isNullOrEmpty(name);
    }

    @Override
    public S3Path next() {
      int i = name.indexOf('/');
      String component;
      if (i >= 0) {
        component = name.substring(0, i);
        name = name.substring(i + 1);
      } else {
        component = name;
        name = null;
      }
      if (fullPath) {
        fullPath = false;
        return new S3Path(innerFileSystem, component, "");
      } else {
        // Relative paths have no bucket.
        return new S3Path(innerFileSystem, "", component);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  public S3Path commonKeyPrefix(S3Path other) {
    checkArgument(getBucket().equals(other.getBucket()), "buckets equal");

    int minLength = Math.min(getKey().length(), other.getKey().length());
    String commonKeyPrefix = getKey().substring(0, minLength);
    for (int i = 0; i < minLength; i++) {
      if (getKey().charAt(i) != other.getKey().charAt(i)) {
        commonKeyPrefix = getKey().substring(0, i);
        break;
      }
    }

    return new S3Path(null, getBucket(), commonKeyPrefix);
  }

  @Override
  public int compareTo(Path other) {
    if (!(other instanceof S3Path)) {
      throw new ClassCastException();
    }

    S3Path path = (S3Path) other;
    int b = bucket.compareTo(path.bucket);
    if (b != 0) {
      return b;
    }

    // Compare a component at a time, so that the separator char doesn't
    // get compared against component contents.  Eg, "a/b" < "a-1/b".
    Iterator<Path> left = iterator();
    Iterator<Path> right = path.iterator();

    while (left.hasNext() && right.hasNext()) {
      String leftStr = left.next().toString();
      String rightStr = right.next().toString();
      int c = leftStr.compareTo(rightStr);
      if (c != 0) {
        return c;
      }
    }

    if (!left.hasNext() && !right.hasNext()) {
      return 0;
    } else {
      return left.hasNext() ? 1 : -1;
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    S3Path otherS3Path = (S3Path) other;
    return bucket.equals(otherS3Path.bucket) && key.equals(otherS3Path.key);
  }

  @Override
  public int hashCode() {
    return 31 * bucket.hashCode() + key.hashCode();
  }

  @Override
  public String toString() {
    if (!isAbsolute()) {
      return key;
    }
    StringBuilder sb = new StringBuilder();
    sb.append(SCHEME).append("://");
    if (!bucket.isEmpty()) {
      sb.append(bucket).append('/');
    }
    sb.append(key);
    return sb.toString();
  }

  private String bucketAndKey() {
    if (bucket.isEmpty()) {
      return key;
    }
    return bucket + "/" + key;
  }

  public String getKeyNonWildcardPrefix() {
    Matcher m = GLOB_PREFIX.matcher(getKey());
    checkArgument(m.matches(), String.format("Glob expression: [%s] is not expandable.", getKey()));
    return m.group("PREFIX");
  }

  /** A coder for {@link S3Path}. */
  public static class Coder extends SerializableCoder<S3Path> {

    private static final Coder CODER_INSTANCE = new Coder();

    public static Coder of() {
      return CODER_INSTANCE;
    }

    private Coder() {
      super(S3Path.class, TypeDescriptor.of(S3Path.class));
    }

    @Override
    public void verifyDeterministic() {}
  }
}
