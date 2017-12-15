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
package org.apache.beam.sdk.fn.data;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.stream.StreamObserverFactory.StreamObserverClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A gRPC multiplexer for a specific {@link Endpoints.ApiServiceDescriptor}.
 *
 * <p>Multiplexes data for inbound consumers based upon their individual {@link
 * org.apache.beam.model.fnexecution.v1.BeamFnApi.Target}s.
 *
 * <p>Multiplexing inbound and outbound streams is as thread safe as the consumers of those streams.
 * For inbound streams, this is as thread safe as the inbound observers. For outbound streams, this
 * is as thread safe as the underlying stream observer.
 *
 * <p>TODO: Add support for multiplexing over multiple outbound observers by stickying the output
 * location with a specific outbound observer.
 */
public class BeamFnDataGrpcMultiplexer {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataGrpcMultiplexer.class);
  @Nullable private final Endpoints.ApiServiceDescriptor apiServiceDescriptor;
  private final StreamObserver<BeamFnApi.Elements> inboundObserver;
  private final StreamObserver<BeamFnApi.Elements> outboundObserver;
  private final ConcurrentMap<
            LogicalEndpoint, SettableFuture<DataBytesReceiver>>
      consumers;

  public BeamFnDataGrpcMultiplexer(
      @Nullable Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      StreamObserverClientFactory<BeamFnApi.Elements, BeamFnApi.Elements> outboundObserverFactory) {
    this.apiServiceDescriptor = apiServiceDescriptor;
    this.consumers = new ConcurrentHashMap<>();
    this.inboundObserver = new InboundObserver();
    this.outboundObserver = outboundObserverFactory.outboundObserverFor(inboundObserver);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("apiServiceDescriptor", apiServiceDescriptor)
        .add("consumers", consumers)
        .toString();
  }

  public StreamObserver<BeamFnApi.Elements> getInboundObserver() {
    return inboundObserver;
  }

  public StreamObserver<BeamFnApi.Elements> getOutboundObserver() {
    return outboundObserver;
  }

  private SettableFuture<DataBytesReceiver> receiverFuture(LogicalEndpoint endpoint) {
    SettableFuture<DataBytesReceiver> future = consumers.get(endpoint);
    if (future == null) {
      future = SettableFuture.create();
      SettableFuture<DataBytesReceiver> present =
          consumers.putIfAbsent(endpoint, future);
      if (present != null) {
        // This call lost the race; use the existing future
        future = present;
      }
    }
    return future;
  }

  public void registerReceiver(LogicalEndpoint inputLocation, DataBytesReceiver dataBytesReceiver) {
    receiverFuture(inputLocation).set(dataBytesReceiver);
  }

  @VisibleForTesting
  boolean hasReceiver(LogicalEndpoint outputLocation) {
    return consumers.containsKey(outputLocation);
  }

  /**
   * A multiplexing {@link StreamObserver} that selects the inbound {@link DataBytesReceiver} to
   * pass the elements to.
   *
   * <p>The inbound observer blocks until the {@link DataBytesReceiver} is bound allowing for the
   * sending harness to initiate transmitting data without needing for the receiving harness to
   * signal that it is ready to consume that data.
   */
  private final class InboundObserver implements StreamObserver<BeamFnApi.Elements> {
    @Override
    public void onNext(BeamFnApi.Elements value) {
      for (BeamFnApi.Elements.Data data : value.getDataList()) {
        try {
          LogicalEndpoint key =
              LogicalEndpoint.of(data.getInstructionReference(), data.getTarget());
          SettableFuture<DataBytesReceiver> consumer = receiverFuture(key);
          if (!consumer.isDone()) {
            LOG.debug("Received data for key {} without consumer ready. "
                + "Waiting for consumer to be registered.", key);
          }
          consumer.get().receive(data);
          if (data.getData().isEmpty()) {
            consumers.remove(key);
          }
        /*
         * TODO: On failure we should fail any bundles that were impacted eagerly
         * instead of relying on the Runner harness to do all the failure handling.
         */
        } catch (ExecutionException | InterruptedException e) {
          LOG.error(
              "Client interrupted during handling of data for instruction {} and target {}",
              data.getInstructionReference(),
              data.getTarget(),
              e);
          outboundObserver.onError(e);
        } catch (RuntimeException e) {
          LOG.error(
              "Client failed to handle data for instruction {} and target {}",
              data.getInstructionReference(),
              data.getTarget(),
              e);
          outboundObserver.onError(e);
        }
      }
    }

    @Override
    public void onError(Throwable t) {
      LOG.error(
          "Failed to handle for {}",
          apiServiceDescriptor == null ? "unknown endpoint" : apiServiceDescriptor,
          t);
    }

    @Override
    public void onCompleted() {
      LOG.warn(
          "Hanged up for {}.",
          apiServiceDescriptor == null ? "unknown endpoint" : apiServiceDescriptor);
    }
  }
}
