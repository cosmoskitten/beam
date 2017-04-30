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
package org.apache.beam.integration.nexmark.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.integration.nexmark.NexmarkUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/**
 * Result of query 10.
 */
public class Done implements KnownSize, Serializable {
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  public static final Coder<Done> CODER = new CustomCoder<Done>() {
    @Override
    public void encode(Done value, OutputStream outStream,
        Coder.Context context)
        throws CoderException, IOException {
      STRING_CODER.encode(value.message, outStream, Context.NESTED);
    }

    @Override
    public Done decode(
        InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      String message = STRING_CODER.decode(inStream, Context.NESTED);
      return new Done(message);
    }
    @Override public void verifyDeterministic() throws NonDeterministicException {}
  };

  @JsonProperty
  private final String message;

  // For Avro only.
  @SuppressWarnings("unused")
  public Done() {
    message = null;
  }

  public Done(String message) {
    this.message = message;
  }

  @Override
  public long sizeInBytes() {
    return message.length();
  }

  @Override
  public String toString() {
    try {
      return NexmarkUtils.MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
