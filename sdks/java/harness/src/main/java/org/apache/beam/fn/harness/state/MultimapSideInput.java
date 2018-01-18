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
package org.apache.beam.fn.harness.state;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.function.Supplier;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.stream.DataStreams;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;

/**
 * An implementation of a multimap side input that utilizes the Beam Fn State API to fetch values.
 *
 * <p>TODO: Support block level caching and prefetch.
 */
public class MultimapSideInput<K, V> implements MultimapView<K, V> {
  private final BeamFnStateClient beamFnStateClient;
  private final String sideInputId;
  private final Coder<K> keyCoder;
  private final Coder<V> valueCoder;
  private final Supplier<StateRequest.Builder> partialRequestSupplier;

  public MultimapSideInput(
      BeamFnStateClient beamFnStateClient,
      String sideInputId,
      Coder<K> keyCoder,
      Coder<V> valueCoder,
      Supplier<StateRequest.Builder> partialRequestSupplier) {
    this.beamFnStateClient = beamFnStateClient;
    this.sideInputId = sideInputId;
    this.keyCoder = keyCoder;
    this.valueCoder = valueCoder;
    this.partialRequestSupplier = partialRequestSupplier;
  }

  public Iterable<V> get(K k) {
    return new LazyCachingIteratorToIterable<>(
        new DataStreams.DataStreamDecoder(valueCoder,
            DataStreams.inbound(
                StateFetchingIterators.usingPartialRequestWithStateKey(
                    beamFnStateClient,
                    new RequestSupplierForKey(k)))));
  }

  /** Lazily encodes and caches the key on first use. */
  private class RequestSupplierForKey implements Supplier<StateRequest.Builder> {
    private final K k;
    private ByteString encodedK;

    private RequestSupplierForKey(K k) {
      this.k = k;
    }

    @Override
    public StateRequest.Builder get() {
      if (encodedK == null) {
        ByteString.Output output = ByteString.newOutput();
        try {
          keyCoder.encode(k, output);
        } catch (IOException e) {
          throw new IllegalStateException(
              String.format("Failed to encode key %s for side input id %s.", k, sideInputId),
              e);
        }
        encodedK = output.toByteString();
      }
      StateRequest.Builder builder = partialRequestSupplier.get();
      builder.getStateKeyBuilder().getMultimapSideInputBuilder().setKey(encodedK);
      return builder;
    }
  }
}
