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
package org.apache.beam.sdk.io.aws.options;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests {@link S3Module}. */
@RunWith(JUnit4.class)
public class S3ModuleTest {
  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new S3Module());

  @Test
  public void testObjectMapperIsAbleToFindModule() {
    List<Module> modules = ObjectMapper.findModules(ReflectHelpers.findClassLoader());
    assertThat(modules, hasItem(Matchers.instanceOf(S3Module.class)));
  }

  @Test
  public void testSSECustomerKeySerializationDeserialization() throws Exception {
    final String key = "86glyTlCNZgccSxW8JxMa6ZdjdK3N141glAysPUZ3AA=";
    final String md5 = null;
    final String algorithm = "AES256";

    SSECustomerKey value = new SSECustomerKey(key);
    value.setAlgorithm(algorithm);
    value.setMd5(null);

    String valueAsJson = objectMapper.writeValueAsString(value);
    SSECustomerKey valueDes = objectMapper.readValue(valueAsJson, SSECustomerKey.class);
    assertEquals(key, valueDes.getKey());
    assertEquals(algorithm, valueDes.getAlgorithm());
    assertEquals(md5, valueDes.getMd5());
  }

  @Test
  public void testSSEAwsKeyManagementParamsSerializationDeserialization() throws Exception {
    final String awsKmsKeyId =
        "arn:aws:kms:eu-west-1:123456789012:key/dc123456-7890-ABCD-EF01-234567890ABC";

    SSEAwsKeyManagementParams value = new SSEAwsKeyManagementParams(awsKmsKeyId);

    String valueAsJson = objectMapper.writeValueAsString(value);
    SSEAwsKeyManagementParams valueDes =
        objectMapper.readValue(valueAsJson, SSEAwsKeyManagementParams.class);
    assertEquals(awsKmsKeyId, valueDes.getAwsKmsKeyId());
  }
}
