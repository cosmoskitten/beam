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
package org.apache.beam.sdk.extensions.smb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.smb.avro.AvroBucketMetadata;
import org.apache.beam.sdk.extensions.smb.json.JsonBucketMetadata;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v20_0.com.google.common.hash.HashFunction;
import org.apache.beam.vendor.guava.v20_0.com.google.common.hash.Hashing;

/**
 * Represents SMB metadata in a JSON-serializable format to be stored along with bucketed data.
 *
 * @param <KeyT>
 * @param <ValueT>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = AvroBucketMetadata.class),
  @JsonSubTypes.Type(value = JsonBucketMetadata.class)
})
public abstract class BucketMetadata<KeyT, ValueT> implements Serializable {

  @JsonProperty private final int numBuckets;

  @JsonProperty private final int numShards;

  @JsonProperty private final Class<KeyT> keyClass;

  @JsonProperty private final HashType hashType;

  @JsonIgnore private final HashFunction hashFunction;

  @JsonIgnore private final Coder<KeyT> keyCoder;

  public BucketMetadata(int numBuckets, int numShards, Class<KeyT> keyClass, HashType hashType)
      throws CannotProvideCoderException {
    Preconditions.checkArgument(
        numBuckets > 0 && ((numBuckets & (numBuckets - 1)) == 0),
        "numBuckets must be a power of 2");
    Preconditions.checkArgument(numShards > 0, "numShards must be > 0");

    this.numBuckets = numBuckets;
    this.numShards = numShards;
    this.keyClass = keyClass;
    this.hashType = hashType;
    this.hashFunction = hashType.create();
    this.keyCoder = getKeyCoder();
  }

  @SuppressWarnings("unchecked")
  @JsonIgnore
  Coder<KeyT> getKeyCoder() throws CannotProvideCoderException {
    final Coder overriddenCoder = coderOverrides().get(getKeyClass());

    if (overriddenCoder != null) {
      return (Coder<KeyT>) overriddenCoder;
    } else {
      return CoderRegistry.createDefault().getCoder(keyClass);
    }
  }

  @JsonIgnore
  protected Map<Class<?>, Coder<?>> coderOverrides() {
    return Collections.emptyMap();
  }

  /** Enumerated hashing schemes available for an SMB sink. */
  public enum HashType {
    MURMUR3_32 {
      @Override
      public HashFunction create() {
        return Hashing.murmur3_32();
      }
    },
    MURMUR3_128 {
      @Override
      public HashFunction create() {
        return Hashing.murmur3_128();
      }
    };

    public abstract HashFunction create();
  }

  // Todo: support sources with different # buckets.
  @VisibleForTesting
  public boolean compatibleWith(BucketMetadata other) {
    return other != null && this.hashType == other.hashType && this.numBuckets == other.numBuckets;
  }

  ////////////////////////////////////////
  // Configuration
  ////////////////////////////////////////

  public int getNumBuckets() {
    return numBuckets;
  }

  public int getNumShards() {
    return numShards;
  }

  public Class<KeyT> getKeyClass() {
    return keyClass;
  }

  public HashType getHashType() {
    return hashType;
  }

  ////////////////////////////////////////
  // Business logic
  ////////////////////////////////////////

  byte[] getKeyBytes(ValueT value) {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final KeyT key = extractKey(value);
    try {
      keyCoder.encode(key, baos);
    } catch (Exception e) {
      throw new RuntimeException("Could not encode key " + key, e);
    }

    return baos.toByteArray();
  }

  public abstract KeyT extractKey(ValueT value);

  int getBucketId(byte[] keyBytes) {
    return Math.abs(hashFunction.hashBytes(keyBytes).asInt()) % numBuckets;
  }

  ////////////////////////////////////////
  // Serialization
  ////////////////////////////////////////

  @JsonIgnore private static ObjectMapper objectMapper = new ObjectMapper();

  @VisibleForTesting
  public static <KeyT, ValueT> BucketMetadata<KeyT, ValueT> from(String src) throws IOException {
    return objectMapper.readerFor(BucketMetadata.class).readValue(src);
  }

  @VisibleForTesting
  public static <KeyT, ValueT> BucketMetadata<KeyT, ValueT> from(InputStream src)
      throws IOException {
    return objectMapper.readerFor(BucketMetadata.class).readValue(src);
  }

  @VisibleForTesting
  public static <KeyT, ValueT> void to(
      BucketMetadata<KeyT, ValueT> bucketMetadata, OutputStream outputStream) throws IOException {

    objectMapper.writeValue(outputStream, bucketMetadata);
  }

  @Override
  public String toString() {
    try {
      return objectMapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  static class BucketMetadataCoder<K, V> extends AtomicCoder<BucketMetadata<K, V>> {
    private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();

    @Override
    public void encode(BucketMetadata<K, V> value, OutputStream outStream)
        throws CoderException, IOException {
      stringCoder.encode(value.toString(), outStream);
    }

    @Override
    public BucketMetadata<K, V> decode(InputStream inStream) throws CoderException, IOException {
      return BucketMetadata.from(stringCoder.decode(inStream));
    }
  }
}
