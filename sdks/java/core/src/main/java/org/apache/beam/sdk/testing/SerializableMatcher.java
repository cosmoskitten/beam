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
package org.apache.beam.sdk.testing;

import com.google.api.client.util.Base64;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.beam.sdk.testing.SerializableMatcher.MatcherDeserializer;
import org.apache.beam.sdk.testing.SerializableMatcher.MatcherSerializer;
import org.apache.beam.sdk.util.SerializableUtils;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.Serializable;

/**
 * A {@link Matcher} that is also {@link Serializable}.
 *
 * <p>Such matchers can be used with {@link PAssert}, which builds Dataflow pipelines
 * such that these matchers may be serialized and executed remotely.
 *
 * <p>To create a {@code SerializableMatcher}, extend {@link org.hamcrest.BaseMatcher}
 * and also implement this interface.
 *
 * @param <T> The type of value matched.
 */
@JsonSerialize(using = MatcherSerializer.class)
@JsonDeserialize(using = MatcherDeserializer.class)
public interface SerializableMatcher<T> extends Matcher<T>, Serializable {
  class MatcherSerializer extends JsonSerializer<SerializableMatcher<?>> {
    @Override
    public void serialize(SerializableMatcher<?> matcher, JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
      byte[] out = SerializableUtils.serializeToByteArray(matcher);
      jsonGenerator.writeStartObject();
      jsonGenerator.writeBinaryField("matcher", out);
      jsonGenerator.writeEndObject();
    }
  }

  class MatcherDeserializer extends JsonDeserializer<SerializableMatcher<?>> {
    @Override
    public SerializableMatcher<?> deserialize(JsonParser jsonParser,
        DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException {
      ObjectNode node = jsonParser.readValueAsTree();
      String matcher = node.get("matcher").asText();
      byte[] in = Base64.decodeBase64(matcher);
      return (SerializableMatcher<?>) SerializableUtils
          .deserializeFromByteArray(in, "SerializableMatcher");
    }
  }
}

