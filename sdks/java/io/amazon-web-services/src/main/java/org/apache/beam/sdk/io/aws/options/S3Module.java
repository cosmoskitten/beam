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

import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.auto.service.AutoService;
import java.io.IOException;

/**
 * A Jackson {@link Module} that registers a {@link JsonDeserializer} for {@link SSECustomerKey} and
 * {@link SSEAwsKeyManagementParams}. The serialized form is based on a Bean serialization.
 */
@AutoService(Module.class)
public class S3Module extends SimpleModule {

  public S3Module() {
    super("S3Module");
    setMixInAnnotation(SSECustomerKey.class, SSECustomerKeyMixin.class);
    setMixInAnnotation(SSEAwsKeyManagementParams.class, SSEAwsKeyManagementParamsMixin.class);
  }

  /** A mixin to add Jackson annotations to {@link SSECustomerKey}. */
  @JsonDeserialize(using = SSECustomerKeyDeserializer.class)
  private static class SSECustomerKeyMixin {}

  private static class SSECustomerKeyDeserializer extends JsonDeserializer<SSECustomerKey> {
    @Override
    public SSECustomerKey deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      JsonNode node = parser.getCodec().readTree(parser);
      final String key = node.get("key") != null ? node.get("key").textValue() : null;
      final String algorithm =
          node.get("algorithm") != null ? node.get("algorithm").textValue() : null;
      final String md5 = node.get("md5") != null ? node.get("md5").textValue() : null;
      return new SSECustomerKey(key).withAlgorithm(algorithm).withMd5(md5);
    }
  }

  /** A mixin to add Jackson annotations to {@link SSEAwsKeyManagementParams}. */
  @JsonDeserialize(using = SSEAwsKeyManagementParamsDeserializer.class)
  private static class SSEAwsKeyManagementParamsMixin {}

  private static class SSEAwsKeyManagementParamsDeserializer
      extends JsonDeserializer<SSEAwsKeyManagementParams> {
    @Override
    public SSEAwsKeyManagementParams deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      JsonNode node = parser.getCodec().readTree(parser);
      final String awsKmsKeyId =
          node.get("awsKmsKeyId") != null ? node.get("awsKmsKeyId").textValue() : null;
      return new SSEAwsKeyManagementParams(awsKmsKeyId);
    }
  }
}
