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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.ResourceId;

/**
 * File system interface in Beam.
 *
 * <p>It defines APIs for writing file systems agnostic code.
 *
 * <p>All methods are protected, and they are for file system providers to implement.
 * Clients should use {@link FileSystems} utility.
 */
public abstract class FileSystem<ResourceIdT extends ResourceId> {

  /**
   * Returns a write channel for the given file.
   *
   * <p>The file is not expanded; it is used verbatim.
   */
  protected abstract WritableByteChannel create(
      ResourceIdT resourceId, CreateOptions createOptions) throws IOException;

  /**
   * Returns a read channel for the given file.
   *
   * <p>The file is not expanded; it is used verbatim.
   *
   * <p>If seeking is supported, then this returns a
   * {@link java.nio.channels.SeekableByteChannel}.
   * @param resourceId
   */
  protected abstract ReadableByteChannel open(ResourceIdT resourceId) throws IOException;

  /**
   * Copies a {@link List} of files from one location to another.
   *
   * <p>The number of source files must equal the number of destination files.
   * Destination files will be created recursively.
   *
   * @param srcResourceIds the source files.
   * @param destResourceIds the destination files.
   *
   * @throws FileNotFoundException if the source files are missing. When it throws,
   * each file might or might not be copied. In such scenarios, callers can use {@code match()}
   * to determine the state of the files.
   */
  protected abstract void copy(
      List<ResourceIdT> srcResourceIds,
      List<ResourceIdT> destResourceIds) throws IOException;

  /**
   * Renames a {@link List} of files from one location to another.
   *
   * <p>The number of source files must equal the number of destination files.
   * Destination files will be created recursively.
   *
   * @param srcResourceIds the source files.
   * @param destResourceIds the destination files.
   *
   * @throws FileNotFoundException if the source files are missing. When rename throws,
   * the state of the files is unknown but safe:
   * for every (source, destination) pair of files, the following are possible:
   * a) source exists, b) destination exists, c) source and destination both exist.
   * Thus no data is lost, however, duplicated files are possible.
   * In such scenarios, callers can use {@code match()} to determine the state of the files.
   */
  protected abstract void rename(
      List<ResourceIdT> srcResourceIds,
      List<ResourceIdT> destResourceIds) throws IOException;

  /**
   * Deletes a collection of files (including directories).
   *
   * <p>It is allowed but not recommended to delete directories recursively.
   * Callers depends on {@link FileSystems} and uses {@code DeleteOptions}.
   *
   * @param resourceIds the files to delete.
   *
   * @throws FileNotFoundException if files are missing. When it throws,
   * each file might or might not be deleted. In such scenarios, callers can use {@code match()}
   * to determine the state of the files.
   */
  protected abstract void delete(Collection<ResourceIdT> resourceIds) throws IOException;
}
