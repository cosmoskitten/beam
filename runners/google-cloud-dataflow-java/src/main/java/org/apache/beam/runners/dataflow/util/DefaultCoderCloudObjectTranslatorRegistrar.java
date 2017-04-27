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

package org.apache.beam.runners.dataflow.util;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.beam.runners.dataflow.internal.IsmFormat.FooterCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmShardCoder;
import org.apache.beam.runners.dataflow.internal.IsmFormat.KeyPrefixCoder;
import org.apache.beam.runners.dataflow.util.RandomAccessData.RandomAccessDataCoder;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.BigIntegerCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.DurationCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TextualIntegerCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder;
import org.apache.beam.sdk.io.FileBasedSink.FileResultCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.util.BitSetCoder;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;

/**
 * The {@link CoderCloudObjectTranslatorRegistrar} containing the default collection of
 * {@link Coder} {@link CloudObjectTranslator Cloud Object Translators}.
 */
@AutoService(CoderCloudObjectTranslatorRegistrar.class)
public class DefaultCoderCloudObjectTranslatorRegistrar
    implements CoderCloudObjectTranslatorRegistrar {
  private static final ImmutableMap<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>
      DEFAULT_TRANSLATORS =
          ImmutableMap.<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>builder()
              .put(GlobalWindow.Coder.class, CloudObjectTranslators.globalWindow())
              .put(IntervalWindowCoder.class, CloudObjectTranslators.intervalWindow())
              .put(ByteArrayCoder.class, CloudObjectTranslators.bytes())
              .put(VarLongCoder.class, CloudObjectTranslators.varInt())
              .put(LengthPrefixCoder.class, CloudObjectTranslators.lengthPrefix())
              .put(IterableCoder.class, CloudObjectTranslators.stream())
              .put(KvCoder.class, CloudObjectTranslators.pair())
              .put(FullWindowedValueCoder.class, CloudObjectTranslators.windowedValue())
              .put(CustomCoder.class, CloudObjectTranslators.custom())
              .build();
  @VisibleForTesting
  static final ImmutableSet<Class<? extends Coder>> KNOWN_ATOMIC_CODERS =
      ImmutableSet.<Class<? extends Coder>>of(IsmShardCoder.class,
          KeyPrefixCoder.class,
          FooterCoder.class,
          RandomAccessDataCoder.class,
          BigDecimalCoder.class,
          BigEndianIntegerCoder.class,
          BigEndianLongCoder.class,
          BigIntegerCoder.class,
          ByteCoder.class,
          ByteStringCoder.class,
          DoubleCoder.class,
          DurationCoder.class,
          InstantCoder.class,
          StringUtf8Coder.class,
          TextualIntegerCoder.class,
          VarIntCoder.class,
          VoidCoder.class,
          FileResultCoder.class,
          BitSetCoder.class);
  private static final Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>
      DEFAULT_ATOMIC_CODERS =
          ImmutableMap.copyOf(
              Maps.asMap(
                  KNOWN_ATOMIC_CODERS,
                  new Function<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>() {
                    @Override
                    public CloudObjectTranslator<? extends Coder> apply(
                        Class<? extends Coder> input) {
                      return CloudObjectTranslators.atomic(input);
                    }
                  }));
  // TODO: Atomic, GCPIO Coders:
  // TableDestinationCoder.class,
  // TableRowInfoCoder.class
  // TableRowJsonCoder.class,
  // PubsubUnboundedSink.OutgoingMessageCoder.class,
  // PubsubUnboundedSource.PubsubCheckpointCoder.class,

  // TODO: Coders with a custom wire format.
  // SerializableCoder.class
  // AvroCoder.class
  // ProtoCoder.class
  @Override
  public Map<String, CloudObjectTranslator<? extends Coder>> classNamesToTranslators() {
    ImmutableMap.Builder<String, CloudObjectTranslator<? extends Coder>> nameToTranslators =
        ImmutableMap.builder();
    for (CloudObjectTranslator<? extends Coder> translator : classesToTranslators().values()) {
      nameToTranslators.put(translator.cloudObjectClassName(), translator);
    }
    return nameToTranslators.build();
  }

  @Override
  public Map<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>
      classesToTranslators() {
    return ImmutableMap.<Class<? extends Coder>, CloudObjectTranslator<? extends Coder>>builder()
        .putAll(DEFAULT_TRANSLATORS)
        .putAll(DEFAULT_ATOMIC_CODERS)
        .build();
  }
}
