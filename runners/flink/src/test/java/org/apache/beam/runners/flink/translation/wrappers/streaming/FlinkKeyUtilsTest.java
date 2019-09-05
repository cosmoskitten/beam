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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.nio.ByteBuffer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.sdk.v2.sdk.extensions.protobuf.ByteStringCoder;
import org.junit.Test;

/** Tests for {@link FlinkKeyUtils}. */
public class FlinkKeyUtilsTest {

  @Test
  public void testEncodeDecode() {
    String key = "key";
    StringUtf8Coder coder = StringUtf8Coder.of();

    ByteBuffer byteBuffer = FlinkKeyUtils.encodeKey(key, coder);
    assertThat(FlinkKeyUtils.decodeKey(byteBuffer, coder), is(key));
  }

  @Test
  public void testNullKey() {
    Void key = null;
    VoidCoder coder = VoidCoder.of();

    ByteBuffer byteBuffer = FlinkKeyUtils.encodeKey(key, coder);
    assertThat(FlinkKeyUtils.decodeKey(byteBuffer, coder), is(nullValue()));
  }

  @Test
  @SuppressWarnings("ByteBufferBackingArray")
  public void testCoderContext() throws Exception {
    byte[] bytes = {1, 1, 1};
    ByteString key = ByteString.copyFrom(bytes);
    ByteStringCoder coder = ByteStringCoder.of();

    ByteBuffer encoded = FlinkKeyUtils.encodeKey(key, coder);
    // Ensure outer context is used where no length encoding is used.
    assertThat(encoded.array(), is(bytes));
  }

  @Test
  public void testRemoveNestedContext() {
    byte[] bytes = {2, 23, 42};
    ByteString key = ByteString.copyFrom(bytes);
    Coder byteStringCoder = ByteStringCoder.of();
    assertThat(
        FlinkKeyUtils.removeNestedContext(key, byteStringCoder),
        is(ByteBuffer.wrap(new byte[] {23, 42})));
  }
}
