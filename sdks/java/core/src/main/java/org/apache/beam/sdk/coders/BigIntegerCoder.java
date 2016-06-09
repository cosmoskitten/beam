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
package org.apache.beam.sdk.coders;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;

/**
 * A {@link BigIntegerCoder} encodes a {@link BigInteger} as a length-prefixed
 * byte array containing the big endian two's-complement representation.
 */
public class BigIntegerCoder extends AtomicCoder<BigInteger> {

  @JsonCreator
  public static BigIntegerCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final BigIntegerCoder INSTANCE = new BigIntegerCoder();

  private BigIntegerCoder() {}

  @Override
  public void encode(BigInteger value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    checkNotNull(value, String.format("cannot encode a null %s", BigInteger.class.getSimpleName()));

    byte[] bigIntBytes = value.toByteArray();

    DataOutputStream dataOutputStream = new DataOutputStream(outStream);
    dataOutputStream.writeInt(bigIntBytes.length);
    dataOutputStream.write(bigIntBytes);
  }

  @Override
  public BigInteger decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    DataInputStream dataInputStream = new DataInputStream(inStream);

    int bigIntBytesSize = dataInputStream.readInt();
    byte[] bigIntBytes = new byte[bigIntBytesSize];
    dataInputStream.readFully(bigIntBytes);

    BigInteger bigInteger = new BigInteger(bigIntBytes);

    return bigInteger;
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code true}. This coder is injective.
   */
  @Override
  public boolean consistentWithEquals() {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code true}, because {@link #getEncodedElementByteSize} runs in constant time.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(BigInteger value, Context context) {
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code 4} (the size of a big endian int length prefix) plus the
   * size of the {@link BigInteger} bytes.
   */
  @Override
  protected long getEncodedElementByteSize(BigInteger value, Context context)
      throws Exception {
    checkNotNull(value, String.format("cannot encode a null %s", BigInteger.class.getSimpleName()));
    return 4 + value.toByteArray().length;
  }

}
