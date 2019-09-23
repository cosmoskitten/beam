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

import static org.apache.beam.sdk.io.common.IOITHelper.getHashForRecordCount;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/** Contains helper methods for file based IO Integration tests. */
public class FileBasedIOITHelper {

  private FileBasedIOITHelper() {}

  public static FileBasedIOTestPipelineOptions readFileBasedIOITPipelineOptions() {
    return IOITHelper.readIOTestPipelineOptions(FileBasedIOTestPipelineOptions.class);
  }

  public static String appendTimestampSuffix(String text) {
    return String.format("%s_%s", text, new Date().getTime());
  }

  public static IOTestConfig getTestConfigurationForConfigName(ConfigName name) {
    EnumMap<ConfigName, IOTestConfig> configMap = new EnumMap<>(ConfigName.class);
    configMap.put(
        ConfigName.TEXT_1GB_GZIP,
        new IOTestConfig(
            "8a3de973354abc6fba621c6797cc0f06", 1_097_840_000L, 450_000_000, Compression.GZIP));
    configMap.put(
        ConfigName.TEXT_1GB_UNCOMPRESSED,
        new IOTestConfig(
            "f8453256ccf861e8a312c125dfe0e436",
            1_062_290_000L,
            25_000_000,
            Compression.UNCOMPRESSED));
    configMap.put(
        ConfigName.TFRECORD_1GB,
        new IOTestConfig("543104423f8b6eb097acb9f111c19fe4", 1_019_380_000L, 18_000_000));
    configMap.put(
        ConfigName.AVRO_1GB,
        new IOTestConfig("2f9f5ca33ea464b25109c0297eb6aecb", 1_089_730_000L, 225_000_000));
    configMap.put(
        ConfigName.PARQUET_1GB,
        new IOTestConfig("2f9f5ca33ea464b25109c0297eb6aecb", 1_087_370_000L, 225_000_000));
    configMap.put(
        ConfigName.XML_1GB,
        new IOTestConfig("b3b717e7df8f4878301b20f314512fb3", 1_076_590_000L, 12_000_000));

    IOTestConfig config = configMap.get(name);
    if (config == null) {
      throw new UnsupportedOperationException(
          String.format("No configuration for that config name: %s", name));
    }
    return config;
  }

  public static String getExpectedHashForLineCount(int lineCount) {
    Map<Integer, String> expectedHashes =
        ImmutableMap.of(
            1000, "8604c70b43405ef9803cb49b77235ea2",
            100_000, "4c8bb3b99dcc59459b20fefba400d446",
            1_000_000, "9796db06e7a7960f974d5a91164afff1",
            200_000_000, "6ce05f456e2fdc846ded2abd0ec1de95");

    return getHashForRecordCount(lineCount, expectedHashes);
  }

  /** Constructs text lines in files used for testing. */
  public static class DeterministicallyConstructTestTextLineFn extends DoFn<Long, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(String.format("IO IT Test line of text. Line seed: %s", c.element()));
    }
  }

  /** Deletes matching files using the FileSystems API. */
  public static class DeleteFileFn extends DoFn<String, Void> {

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      MatchResult match =
          Iterables.getOnlyElement(FileSystems.match(Collections.singletonList(c.element())));

      Set<ResourceId> resourceIds = new HashSet<>();
      for (MatchResult.Metadata metadataElem : match.metadata()) {
        resourceIds.add(metadataElem.resourceId());
      }

      FileSystems.delete(resourceIds);
    }
  }
}
