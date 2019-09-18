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
package org.apache.beam.sdk.io.common;

import org.apache.beam.sdk.io.Compression;

public class IOTestConfig {
  private String expectedHash;
  private long datasetSize;
  private int numberOfRecords;
  private Compression compression;

  public IOTestConfig(String expectedHash, long datasetSize, int numberOfRecords) {
    this.expectedHash = expectedHash;
    this.datasetSize = datasetSize;
    this.numberOfRecords = numberOfRecords;
    compression = Compression.UNCOMPRESSED;
  }

  public IOTestConfig(
      String expectedHash, long datasetSize, int numberOfRecords, Compression compression) {
    this.expectedHash = expectedHash;
    this.datasetSize = datasetSize;
    this.numberOfRecords = numberOfRecords;
    this.compression = compression;
  }

  public Compression getCompression() {
    return compression;
  }

  public String getExpectedHash() {
    return expectedHash;
  }

  public long getDatasetSize() {
    return datasetSize;
  }

  public int getNumberOfRecords() {
    return numberOfRecords;
  }
}
