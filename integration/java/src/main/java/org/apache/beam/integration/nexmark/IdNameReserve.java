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

package org.apache.beam.integration.nexmark;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Result type of {@link Query8}.
 */
public class IdNameReserve implements KnownSize, Serializable {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  public static final Coder<IdNameReserve> CODER = new AtomicCoder<IdNameReserve>() {
    @Override
    public void encode(IdNameReserve value, OutputStream outStream,
        Coder.Context context)
        throws CoderException, IOException {
      LONG_CODER.encode(value.id, outStream, Context.NESTED);
      STRING_CODER.encode(value.name, outStream, Context.NESTED);
      LONG_CODER.encode(value.reserve, outStream, Context.NESTED);
    }

    @Override
    public IdNameReserve decode(
        InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      long id = LONG_CODER.decode(inStream, Context.NESTED);
      String name = STRING_CODER.decode(inStream, Context.NESTED);
      long reserve = LONG_CODER.decode(inStream, Context.NESTED);
      return new IdNameReserve(id, name, reserve);
    }
  };

  @JsonProperty
  public final long id;

  @JsonProperty
  public final String name;

  /** Reserve price in cents. */
  @JsonProperty
  public final long reserve;

  // For Avro only.
  @SuppressWarnings("unused")
  private IdNameReserve() {
    id = 0;
    name = null;
    reserve = 0;
  }

  public IdNameReserve(long id, String name, long reserve) {
    this.id = id;
    this.name = name;
    this.reserve = reserve;
  }

  @Override
  public long sizeInBytes() {
    return 8 + name.length() + 1 + 8;
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
