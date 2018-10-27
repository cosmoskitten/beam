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

import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A {@link Coder} for {@code StructuralByteArray}. */
public class StructuralByteArrayCoder extends AtomicCoder<StructuralByteArray> {

  public static StructuralByteArrayCoder of() {
    return INSTANCE;
  }

  private static final StructuralByteArrayCoder INSTANCE = new StructuralByteArrayCoder();
  private static final TypeDescriptor<StructuralByteArray> TYPE_DESCRIPTOR =
      new TypeDescriptor<StructuralByteArray>() {};

  private StructuralByteArrayCoder() {}

  @Override
  public void encode(StructuralByteArray value, OutputStream outStream) throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null StructuralByteArray");
    }
    VarInt.encode(value.getValue().length, outStream);
    outStream.write(value.getValue());
  }

  @Override
  public StructuralByteArray decode(InputStream inStream) throws IOException {
    int length = VarInt.decodeInt(inStream);
    if (length < 0) {
      throw new IOException("invalid length " + length);
    }
    byte[] value = new byte[length];
    ByteStreams.readFully(inStream, value);
    return new StructuralByteArray(value);
  }

  @Override
  public void verifyDeterministic() {}

  @Override
  public boolean consistentWithEquals() {
    return true;
  }

  @Override
  public boolean isRegisterByteSizeObserverCheap(StructuralByteArray value) {
    return true;
  }

  @Override
  public TypeDescriptor<StructuralByteArray> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

  @Override
  protected long getEncodedElementByteSize(StructuralByteArray value) throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null byte[]");
    }
    return (long) VarInt.getLength(value.getValue().length) + value.getValue().length;
  }
}
