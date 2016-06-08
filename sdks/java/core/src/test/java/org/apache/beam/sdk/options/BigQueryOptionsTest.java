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
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.util.IOChannelUtils;

import com.google.api.client.util.Strings;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link BigQueryOptions}.
 */
@RunWith(JUnit4.class)
public class BigQueryOptionsTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testStagingLocation() {
    BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
    IOChannelUtils.registerStandardIOFactories(options);
    options.setTempLocation("file://temp_location");
    options.setBigQueryTempLocation("gs://bq_temp_location");
    assertEquals("gs://bq_temp_location", options.getBigQueryTempLocation());
  }

  @Test
  public void testDefaultStagingLocation() {
    BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
    IOChannelUtils.registerStandardIOFactories(options);
    options.setTempLocation("gs://temp_location");
    assertEquals("gs://temp_location/BigQueryIO", options.getBigQueryTempLocation());
  }

  @Test
  public void testDefaultStagingLocationInvalid() {
    BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
    options.setTempLocation("file://temp_location");
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("GCP temp location requires a valid 'gs://' path");
    options.getBigQueryTempLocation();
  }

  @Test
  public void testDefaultStagingLocationUnset() {
    BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
    assertTrue(Strings.isNullOrEmpty(options.getBigQueryTempLocation()));
  }
}

