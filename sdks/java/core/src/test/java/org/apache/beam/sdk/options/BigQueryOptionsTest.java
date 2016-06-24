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
package org.apache.beam.sdk.options;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.util.IOChannelUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link BigQueryOptions}.
 */
@RunWith(JUnit4.class)
public class BigQueryOptionsTest {

  @Test
  public void testGcpTempLocation() {
    BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
    IOChannelUtils.registerStandardIOFactories(options);
    options.setTempLocation("file://temp_location");
    options.setGcpTempLocation("gs://gcp_temp_location");
    assertEquals("gs://gcp_temp_location", options.getGcpTempLocation());
  }

  @Test
  public void testDefaultGcpTempLocation() {
    BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
    IOChannelUtils.registerStandardIOFactories(options);
    options.setTempLocation("gs://temp_location");
    assertEquals("gs://temp_location", options.getGcpTempLocation());
  }
}

