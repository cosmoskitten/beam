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
package org.apache.beam.runners.reference;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.util.ZipFiles;

/** A utility that replaces directories with a zip file containing its contents. */
class DirectoryZipper {

  /**
   * If the given path is a regular file, returns the file itself. If the given file is a directory,
   * zips the file and returns the path to this new zip file.
   */
  public String replaceDirectoryWithZipFile(String path) throws IOException {
    return zipDirectoryInternal(path);
  }

  private static String zipDirectoryInternal(String path) throws IOException {
    File file = new File(path);
    checkArgument(file.exists(), "No such file: %s", path);
    if (file.isDirectory()) {
      File zipFile = File.createTempFile(file.getName(), ".zip");
      try (FileOutputStream fos = new FileOutputStream(zipFile)) {
        ZipFiles.zipDirectory(file, fos);
      }
      return zipFile.getAbsolutePath();
    } else {
      return path;
    }
  }
}
