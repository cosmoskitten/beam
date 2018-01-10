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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;

/**
 * Utilities for replacing or wrapping unknown coders with {@link LengthPrefixCoder}.
 *
 * <p>TODO: Support a dynamic list of well known coders using either registration or manual listing.
 */
public class LengthPrefixUnknownCoders {
  private static final String BYTES_CODER_TYPE = "urn:beam:coders:bytes:0.1";
  private static final String LENGTH_PREFIX_CODER_TYPE = "urn:beam:coders:length_prefix:0.1";
  private static final Set<String> WELL_KNOWN_CODER_URNS =
      ImmutableSet.of(
          BYTES_CODER_TYPE,
          "urn:beam:coders:kv:0.1",
          "urn:beam:coders:varint:0.1",
          "urn:beam:coders:interval_window:0.1",
          "urn:beam:coders:stream:0.1",
          LENGTH_PREFIX_CODER_TYPE,
          "urn:beam:coders:global_window:0.1",
          "urn:beam:coders:windowed_value:0.1");

  /**
   * Recursively traverse the coder tree and wrap the first unknown coder in every branch with a
   * {@link LengthPrefixCoder} unless an ancestor coder is itself a {@link LengthPrefixCoder}. If
   * {@code replaceWithByteArrayCoder} is set, then replace that unknown coder with a
   * {@link ByteArrayCoder}.
   *
   * @param coderId The root coder contained within {@code coders} to start the recursive descent
   * from.
   * @param components Contains the root coder and all component coders.
   * @param replaceWithByteArrayCoder whether to replace an unknown coder with a
   * {@link ByteArrayCoder}.
   * @return A {@link MessageWithComponents} with the
   * {@link MessageWithComponents#getCoder() root coder} and its component coders.
   */
  public static RunnerApi.MessageWithComponents forCoder(
      String coderId,
      RunnerApi.Components components,
      boolean replaceWithByteArrayCoder) {

    MessageWithComponents.Builder builder = MessageWithComponents.newBuilder();
    RunnerApi.Coder currentCoder = components.getCodersOrThrow(coderId);
    if (LENGTH_PREFIX_CODER_TYPE.equals(currentCoder.getSpec().getSpec().getUrn())) {
      if (replaceWithByteArrayCoder) {
        String lengthPrefixComponentCoderId = generateUniqueId(
            coderId + "-byte_array",
            Sets.union(components.getCodersMap().keySet(),
                builder.getComponents().getCodersMap().keySet()));
        Coder.Builder byteArrayCoder = Coder.newBuilder();
        byteArrayCoder.getSpecBuilder().getSpecBuilder().setUrn(BYTES_CODER_TYPE);
        builder.getComponentsBuilder().putCoders(lengthPrefixComponentCoderId,
            byteArrayCoder.build());
        builder.getCoderBuilder()
            .addComponentCoderIds(lengthPrefixComponentCoderId)
            .getSpecBuilder()
            .getSpecBuilder()
            .setUrn(LENGTH_PREFIX_CODER_TYPE);
      } else {
        builder.setCoder(currentCoder);
        builder.setComponents(components);
      }
    } else if (WELL_KNOWN_CODER_URNS.contains(currentCoder.getSpec().getSpec().getUrn())) {
      RunnerApi.Coder.Builder updatedCoder = currentCoder.toBuilder();
      // Rebuild the component coder ids to handle if any of the component coders changed.
      updatedCoder.clearComponentCoderIds();
      for (final String componentCoderId : currentCoder.getComponentCoderIdsList()) {
        MessageWithComponents componentCoder =
            forCoder(componentCoderId, components, replaceWithByteArrayCoder);
        String newComponentCoderId = componentCoderId;
        if (!components.getCodersOrThrow(componentCoderId).equals(componentCoder.getCoder())) {
          // Generate a new id if the component coder changed.
          newComponentCoderId = generateUniqueId(
              coderId + "-length_prefix",
              Sets.union(components.getCodersMap().keySet(),
                  builder.getComponents().getCodersMap().keySet()));
        }
        updatedCoder.addComponentCoderIds(newComponentCoderId);
        builder.getComponentsBuilder().putCoders(newComponentCoderId, componentCoder.getCoder());
        // Insert all component coders of the component coder.
        builder.getComponentsBuilder().putAllCoders(componentCoder.getComponents().getCodersMap());
      }
      builder.setCoder(updatedCoder);
    } else {
      // If we are handling an unknown URN then we need to wrap it with a length prefix coder.
      // If requested we also replace the unknown coder with a length prefix coder.
      String lengthPrefixComponentCoderId = coderId;
      if (replaceWithByteArrayCoder) {
        lengthPrefixComponentCoderId = generateUniqueId(coderId + "-byte_array",
            Sets.union(components.getCodersMap().keySet(),
                builder.getComponents().getCodersMap().keySet()));
        Coder.Builder byteArrayCoder = Coder.newBuilder();
        byteArrayCoder.getSpecBuilder().getSpecBuilder().setUrn(BYTES_CODER_TYPE);
        builder.getComponentsBuilder().putCoders(lengthPrefixComponentCoderId,
            byteArrayCoder.build());
      } else {
        builder.getComponentsBuilder().putCoders(coderId, currentCoder);
      }

      builder.getCoderBuilder()
          .addComponentCoderIds(lengthPrefixComponentCoderId)
          .getSpecBuilder()
          .getSpecBuilder()
          .setUrn(LENGTH_PREFIX_CODER_TYPE);
    }
    return builder.build();
  }

  /**
   * Generates a unique id given a prefix and the set of existing ids.
   */
  static String generateUniqueId(String prefix, Set<String> existingIds) {
    int i = 0;
    while (existingIds.contains(prefix + i)) {
      i += 1;
    }
    return prefix + i;
  }
}
