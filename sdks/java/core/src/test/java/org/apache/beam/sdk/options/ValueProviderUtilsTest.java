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
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ValueProviderUtils}. */
@RunWith(JUnit4.class)
public class ValueProviderUtilsTest {
  /** A test interface. */
  public interface TestOptions extends PipelineOptions {
    String getString();
    void setString(String value);

    String getOtherString();
    void setOtherString(String value);
  }

  @Test
  public void testUpdateSerialize() throws Exception {
    TestOptions submitOptions = PipelineOptionsFactory.as(TestOptions.class);
    ObjectMapper mapper = new ObjectMapper();
    String serializedOptions = mapper.writeValueAsString(submitOptions);
    String updatedOptions = ValueProviderUtils.updateSerializedOptions(
      serializedOptions, ImmutableMap.of("string", "bar"));
    TestOptions runtime = mapper.readValue(updatedOptions, PipelineOptions.class)
      .as(TestOptions.class);
    assertEquals("bar", runtime.getString());
  }

  @Test
  public void testUpdateSerializeExistingValue() throws Exception {
    TestOptions submitOptions = PipelineOptionsFactory.fromArgs(
      new String[]{"--string=baz", "--otherString=quux"}).as(TestOptions.class);
    ObjectMapper mapper = new ObjectMapper();
    String serializedOptions = mapper.writeValueAsString(submitOptions);
    String updatedOptions = ValueProviderUtils.updateSerializedOptions(
      serializedOptions, ImmutableMap.of("string", "bar"));
    TestOptions runtime = mapper.readValue(updatedOptions, PipelineOptions.class)
      .as(TestOptions.class);
    assertEquals("bar", runtime.getString());
    assertEquals("quux", runtime.getOtherString());
  }

  @Test
  public void testUpdateSerializeEmptyUpdate() throws Exception {
    TestOptions submitOptions = PipelineOptionsFactory.as(TestOptions.class);
    ObjectMapper mapper = new ObjectMapper();
    String serializedOptions = mapper.writeValueAsString(submitOptions);
    String updatedOptions = ValueProviderUtils.updateSerializedOptions(
      serializedOptions, ImmutableMap.<String, String>of());
    TestOptions runtime = mapper.readValue(updatedOptions, PipelineOptions.class)
      .as(TestOptions.class);
    assertNull(runtime.getString());
  }
}
