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

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class CoderTestUtils {

  public static <T> void testConsistentWithEquals(Coder<T> coder, T example) {
    assumeTrue(coder.consistentWithEquals());

    byte[] bytes = encodeBytes(coder, example);

    // even if the coder is non-deterministic, if the encoded bytes match,
    // coder is consistent with equals, decoded values must be equal

    T out0 = decodeBytes(coder, bytes);
    T out1 = decodeBytes(coder, bytes);

    assertEquals("If the encoded bytes match, decoded values must be equal", out0, out1);

    assertEquals(
        "If two values are equal, their hash codes must be equal",
        out0.hashCode(),
        out1.hashCode());
  }

  public static <T> void testStructuralValueConsistentWithEquals(Coder<T> coder, T example) {
    byte[] bytes = encodeBytes(coder, example);

    // even if coder is non-deterministic, if the encoded bytes match,
    // structural values must be equal

    Object out0 = coder.structuralValue(decodeBytes(coder, bytes));
    Object out1 = coder.structuralValue(decodeBytes(coder, bytes));

    assertEquals("If the encoded bytes match, structural values must be equal", out0, out1);

    assertEquals(
        "If two values are equal, their hash codes must be equal",
        out0.hashCode(),
        out1.hashCode());
  }

  private static <T> byte[] encodeBytes(Coder<T> coder, T example) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      coder.encode(example, bos);
      return bos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static <T> T decodeBytes(Coder<T> coder, byte[] bytes) {
    try {
      ByteArrayInputStream bos = new ByteArrayInputStream(bytes);
      return coder.decode(bos);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
