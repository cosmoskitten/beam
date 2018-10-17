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
package org.apache.beam.runners.spark.translation;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.VarInt;

/**
 * A holder object that lets you serialize and element with a Coder with minimal space wastage.
 * Supports both Kryo and Java serialization.
 *
 * <p>There are two different representations: a deserialized representation and a serialized
 * representation.
 *
 * <p>The deserialized representation stores a Coder and the value. To serialize the value, we write
 * a length-prefixed encoding of value, but do NOT write the Coder used.
 *
 * <p>The serialized representation just reads a byte array - the value is not deserialized fully.
 * In order to get at the deserialized value, the caller must pass the Coder used to create this
 * instance via get(Coder). This reverts the representation back to the deserialized representation.
 *
 * @param <T> element type
 */
public class ValueAndCoderKryoSerializable<T> implements KryoSerializable, Externalizable {
  private T value;
  // Re-use a field to save space in-memory. This is either a byte[] or a Coder, depending on
  // which representation we are in.
  private Object coderOrBytes;

  ValueAndCoderKryoSerializable(T value, Coder<T> currentCoder) {
    this.value = value;
    this.coderOrBytes = currentCoder;
  }

  @SuppressWarnings("unused") // for serialization
  public ValueAndCoderKryoSerializable() {}

  public T get(Coder<T> coder) throws IOException {
    if (!(coderOrBytes instanceof Coder)) {
      value =
          coder.decode(new ByteArrayInputStream((byte[]) this.coderOrBytes), Coder.Context.OUTER);
      this.coderOrBytes = coder;
    }

    return value;
  }

  private void writeCommon(OutputStream out) throws IOException {
    if (!(coderOrBytes instanceof Coder)) {
      byte[] bytes = (byte[]) coderOrBytes;
      VarInt.encode(bytes.length, out);
      out.write(bytes);
    } else {
      int bufferSize = 1024;
      // TODO: use isRegisterByteSizeObserverCheap
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(bufferSize);

      try {
        @SuppressWarnings("unchecked")
        Coder<T> coder = (Coder<T>) coderOrBytes;
        coder.encode(value, bytes, Coder.Context.OUTER);

        VarInt.encode(bytes.size(), out);
        bytes.writeTo(out);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void readCommon(InputStream in) throws IOException {
    int length = VarInt.decodeInt(in);
    byte[] bytes = new byte[length];
    ByteStreams.readFully(in, bytes);
    this.coderOrBytes = bytes;
  }

  @Override
  public void write(Kryo kryo, Output output) {
    try {
      writeCommon(output);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void read(Kryo kryo, Input input) {
    try {
      readCommon(input);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    writeCommon(
        new OutputStream() {
          @Override
          public void write(int b) throws IOException {
            out.write(b);
          }

          @Override
          public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
          }

          @Override
          public void flush() throws IOException {
            out.flush();
          }

          @Override
          public void close() throws IOException {
            out.close();
          }
        });
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException {
    readCommon(
        new InputStream() {
          @Override
          public int read() throws IOException {
            return in.read();
          }

          @Override
          public int read(byte[] b, int off, int len) throws IOException {
            return in.read(b, off, len);
          }

          @Override
          public long skip(long n) throws IOException {
            return in.skip(n);
          }

          @Override
          public int available() throws IOException {
            return in.available();
          }

          @Override
          public void close() throws IOException {
            in.close();
          }
        });
  }
}
