/*
 * Copyright (C) 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.runners.fnexecution.graph;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link LengthPrefixUnknownCoders}. */
@RunWith(JUnit4.class)
public class LengthPrefixUnknownCodersTest {
  private static final Coder<WindowedValue<KV<String, Integer>>> windowedValueCoder =
      WindowedValue.getFullCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
          GlobalWindow.Coder.INSTANCE);

  private static final Coder<WindowedValue<KV<String, Integer>>> prefixedWindowedValueCoder =
      WindowedValue.getFullCoder(KvCoder.of(LengthPrefixCoder.of(StringUtf8Coder.of()),
          LengthPrefixCoder.of(VarIntCoder.of())), GlobalWindow.Coder.INSTANCE);

  private static final Coder<WindowedValue<KV<byte[], byte[]>>>
      prefixedAndReplacedWindowedValueCoder = WindowedValue.getFullCoder(
      KvCoder.of(
          LengthPrefixCoder.of(ByteArrayCoder.of()),
          LengthPrefixCoder.of(ByteArrayCoder.of())),
      GlobalWindow.Coder.INSTANCE);

  /** Test wrapping unknown coders with {@code LengthPrefixCoder}. */
  @Test
  public void testLengthPrefixUnknownCoders() throws Exception {
    assertEqualsAfterLengthPrefixingProtoCoder(
        windowedValueCoder, prefixedWindowedValueCoder, false);
  }

  /** Test bypassing unknown coders that are already wrapped with {@code LengthPrefixCoder}. */
  @Test
  public void testLengthPrefixForLengthPrefixCoder() throws Exception {
    Coder<WindowedValue<KV<String, Integer>>> windowedValueCoder =
        WindowedValue.getFullCoder(KvCoder.of(StringUtf8Coder.of(),
            LengthPrefixCoder.of(VarIntCoder.of())), GlobalWindow.Coder.INSTANCE);

    Coder<WindowedValue<KV<String, Integer>>> expectedCoder =
        WindowedValue.getFullCoder(KvCoder.of(LengthPrefixCoder.of(StringUtf8Coder.of()),
            LengthPrefixCoder.of(VarIntCoder.of())), GlobalWindow.Coder.INSTANCE);

    assertEqualsAfterLengthPrefixingProtoCoder(windowedValueCoder, expectedCoder, false);
  }

  /** Test replacing unknown coders with {@code LengthPrefixCoder<ByteArray>}. */
  @Test
  public void testLengthPrefixAndReplaceUnknownCoder() throws Exception {
    Coder<WindowedValue<KV<String, Integer>>> windowedValueCoder =
        WindowedValue.getFullCoder(KvCoder.of(LengthPrefixCoder.of(StringUtf8Coder.of()),
            VarIntCoder.of()), GlobalWindow.Coder.INSTANCE);

    Coder<WindowedValue<KV<byte[], byte[]>>> expectedCoder = prefixedAndReplacedWindowedValueCoder;

    assertEqualsAfterLengthPrefixingProtoCoder(
        windowedValueCoder, prefixedAndReplacedWindowedValueCoder, true);
  }

  private static void assertEqualsAfterLengthPrefixingProtoCoder(
      Coder<?> original, Coder<?> expected, boolean replaceWithByteArray) throws IOException {
    MessageWithComponents messageWithComponents = CoderTranslation.toProto(original);
    Components.Builder builder = messageWithComponents.getComponents().toBuilder();
    String coderId = LengthPrefixUnknownCoders.generateUniqueId("rootTestId",
        messageWithComponents.getComponents().getCodersMap().keySet());
    builder.putCoders(coderId, messageWithComponents.getCoder());
    MessageWithComponents updatedMessageWithComponents = LengthPrefixUnknownCoders.forCoder(
        coderId, builder.build(), replaceWithByteArray);
    assertEquals(expected,
        CoderTranslation.fromProto(updatedMessageWithComponents.getCoder(),
            RehydratedComponents.forComponents(updatedMessageWithComponents.getComponents())));
  }
}
