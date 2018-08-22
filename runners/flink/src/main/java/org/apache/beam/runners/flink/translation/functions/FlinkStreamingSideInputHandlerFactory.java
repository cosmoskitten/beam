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
package org.apache.beam.runners.flink.translation.functions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.SideInputId;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandlerFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * {@link StateRequestHandler} that uses {@link org.apache.beam.runners.core.SideInputHandler} to
 * access the Flink broadcast state that represents side inputs.
 */
public class FlinkStreamingSideInputHandlerFactory implements SideInputHandlerFactory {

  // Map from side input id to global PCollection id.
  private final Map<SideInputId, PCollectionView<?>> sideInputToCollection;
  private final org.apache.beam.runners.core.SideInputHandler runnerHandler;

  /**
   * Creates a new state handler for the given stage. Note that this requires a traversal of the
   * stage itself, so this should only be called once per stage rather than once per bundle.
   */
  public static FlinkStreamingSideInputHandlerFactory forStage(
      ExecutableStage stage,
      Map<SideInputId, PCollectionView<?>> viewMapping,
      org.apache.beam.runners.core.SideInputHandler runnerHandler) {
    ImmutableMap.Builder<SideInputId, PCollectionView<?>> sideInputBuilder = ImmutableMap.builder();
    for (SideInputReference sideInput : stage.getSideInputs()) {
      SideInputId sideInputId =
          SideInputId.newBuilder()
              .setTransformId(sideInput.transform().getId())
              .setLocalName(sideInput.localName())
              .build();
      sideInputBuilder.put(
          sideInputId,
          checkNotNull(
              viewMapping.get(sideInputId),
              "No side input for %s/%s",
              sideInputId.getTransformId(),
              sideInputId.getLocalName()));
    }

    FlinkStreamingSideInputHandlerFactory factory =
        new FlinkStreamingSideInputHandlerFactory(sideInputBuilder.build(), runnerHandler);
    return factory;
  }

  private FlinkStreamingSideInputHandlerFactory(
      Map<SideInputId, PCollectionView<?>> sideInputToCollection,
      org.apache.beam.runners.core.SideInputHandler runnerHandler) {
    this.sideInputToCollection = sideInputToCollection;
    this.runnerHandler = runnerHandler;
  }

  @Override
  public <T, V, W extends BoundedWindow> SideInputHandler<V, W> forSideInput(
      String transformId,
      String sideInputId,
      RunnerApi.FunctionSpec accessPattern,
      Coder<T> elementCoder,
      Coder<W> windowCoder) {

    PCollectionView collectionNode =
        sideInputToCollection.get(
            SideInputId.newBuilder().setTransformId(transformId).setLocalName(sideInputId).build());
    checkArgument(collectionNode != null, "No side input for %s/%s", transformId, sideInputId);

    if (PTransformTranslation.ITERABLE_SIDE_INPUT.equals(accessPattern.getUrn())) {
      @SuppressWarnings("unchecked") // T == V
      Coder<V> outputCoder = (Coder<V>) elementCoder;
      return forIterableSideInput(collectionNode, outputCoder, windowCoder);
    } else if (PTransformTranslation.MULTIMAP_SIDE_INPUT.equals(accessPattern.getUrn())
        || Materializations.MULTIMAP_MATERIALIZATION_URN.equals(accessPattern.getUrn())) {
      // TODO: Remove non standard URN.
      // Using non standard version of multimap urn as dataflow uses the non standard urn.
      @SuppressWarnings("unchecked") // T == KV<?, V>
      KvCoder<?, V> kvCoder = (KvCoder<?, V>) elementCoder;
      return forMultimapSideInput(
          collectionNode, kvCoder.getKeyCoder(), kvCoder.getValueCoder(), windowCoder);
    } else {
      throw new IllegalArgumentException(
          String.format("Unknown side input access pattern: %s", accessPattern));
    }
  }

  private <T, W extends BoundedWindow> SideInputHandler<T, W> forIterableSideInput(
      PCollectionView<?> collection, Coder<T> elementCoder, Coder<W> windowCoder) {

    return new SideInputHandler<T, W>() {
      @Override
      public Iterable<T> get(byte[] key, W window) {
        return (Iterable<T>) runnerHandler.getIterable(collection, window);
      }

      @Override
      public Coder<T> resultCoder() {
        return elementCoder;
      }
    };
  }

  private <K, V, W extends BoundedWindow> SideInputHandler<V, W> forMultimapSideInput(
      PCollectionView<?> collection, Coder<K> keyCoder, Coder<V> valueCoder, Coder<W> windowCoder) {
    /*
    ImmutableMultimap.Builder<SideInputKey, V> multimap = ImmutableMultimap.builder();
        for (WindowedValue<KV<K, V>> windowedValue : broadcastVariable) {
          K key = windowedValue.getValue().getKey();
          V value = windowedValue.getValue().getValue();

          for (BoundedWindow boundedWindow : windowedValue.getWindows()) {
            @SuppressWarnings("unchecked")
            W window = (W) boundedWindow;
            multimap.put(
                SideInputKey.of(keyCoder.structuralValue(key), windowCoder.structuralValue(window)),
                value);
          }
        }

        return new MultimapSideInputHandler(multimap.build(), keyCoder, valueCoder, windowCoder);
    */
    throw new UnsupportedOperationException();
  }

  private static class MultimapSideInputHandler<K, V, W extends BoundedWindow>
      implements SideInputHandler<V, W> {

    private final Multimap<SideInputKey, V> collection;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final Coder<W> windowCoder;

    private MultimapSideInputHandler(
        Multimap<SideInputKey, V> collection,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        Coder<W> windowCoder) {
      this.collection = collection;
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.windowCoder = windowCoder;
    }

    @Override
    public Iterable<V> get(byte[] keyBytes, W window) {
      K key;
      try {
        // TODO: We could skip decoding and just compare encoded values for deterministic keyCoders.
        key = keyCoder.decode(new ByteArrayInputStream(keyBytes));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return collection.get(
          SideInputKey.of(keyCoder.structuralValue(key), windowCoder.structuralValue(window)));
    }

    @Override
    public Coder<V> resultCoder() {
      return valueCoder;
    }
  }

  @AutoValue
  abstract static class SideInputKey {
    static SideInputKey of(Object key, Object window) {
      return new AutoValue_FlinkStreamingSideInputHandlerFactory_SideInputKey(key, window);
    }

    @Nullable
    abstract Object key();

    abstract Object window();
  }
}
