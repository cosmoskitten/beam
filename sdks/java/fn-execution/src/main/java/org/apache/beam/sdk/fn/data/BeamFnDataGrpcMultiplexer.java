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
import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements.Data;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.vendor.grpc.v1.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1.io.grpc.stub.StreamObserver;
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
public class BeamFnDataGrpcMultiplexer implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataGrpcMultiplexer.class);
  @Nullable private final Endpoints.ApiServiceDescriptor apiServiceDescriptor;
  private final InboundObserver inboundObserver;
  private final StreamObserver<BeamFnApi.Elements> outboundObserver;
  // Logical endpoints to consumers of elements.
  private final ConcurrentMap<LogicalEndpoint, CompletableFuture<Consumer<BeamFnApi.Elements.Data>>>
      consumers;

  // TODO(BEAM-5947) Make sure this is closed properly. It appears that close() is never called.
  public BeamFnDataGrpcMultiplexer(
      @Nullable Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      OutboundObserverFactory outboundObserverFactory,
      OutboundObserverFactory.BasicFactory<Elements, Elements> baseOutboundObserverFactory) {
    this(apiServiceDescriptor, outboundObserverFactory, baseOutboundObserverFactory, false);
  }

  public BeamFnDataGrpcMultiplexer(
      @Nullable Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      OutboundObserverFactory outboundObserverFactory,
      OutboundObserverFactory.BasicFactory<Elements, Elements> baseOutboundObserverFactory,
      boolean enableQueuing) {
    this.apiServiceDescriptor = apiServiceDescriptor;
    this.consumers = new ConcurrentHashMap<>();
    this.inboundObserver = new InboundObserver(enableQueuing);
    this.outboundObserver =
        outboundObserverFactory.outboundObserverFor(baseOutboundObserverFactory, inboundObserver);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .omitNullValues()
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

  private CompletableFuture<Consumer<Data>> receiverFuture(LogicalEndpoint endpoint) {
    return consumers.computeIfAbsent(
        endpoint, (LogicalEndpoint unused) -> new CompletableFuture<>());
  }

  public void registerConsumer(
      LogicalEndpoint inputLocation, Consumer<BeamFnApi.Elements.Data> dataBytesReceiver) {
    receiverFuture(inputLocation).complete(dataBytesReceiver);
    this.inboundObserver.registerConsumer(inputLocation.getInstructionId(), dataBytesReceiver);
  }

  @VisibleForTesting
  boolean hasConsumer(LogicalEndpoint outputLocation) {
    return consumers.containsKey(outputLocation);
  }

  @Override
  public void close() {
    for (CompletableFuture<Consumer<BeamFnApi.Elements.Data>> receiver :
        ImmutableList.copyOf(consumers.values())) {
      // Cancel any observer waiting for the client to complete. If the receiver has already been
      // completed or cancelled, this call will be ignored.
      receiver.cancel(true);
    }
    // Cancel any outbound calls and complete any inbound calls, as this multiplexer is hanging up
    outboundObserver.onError(
        Status.CANCELLED.withDescription("Multiplexer hanging up").asException());
    inboundObserver.onCompleted();
  }

  public void drainAndBlock(String instructionId) {
    this.inboundObserver.drainAndBlock(instructionId);
  }

  /**
   * A multiplexing {@link StreamObserver} that selects the inbound {@link Consumer} to pass the
   * elements to.
   *
   * <p>The inbound observer blocks until the {@link Consumer} is bound allowing for the sending
   * harness to initiate transmitting data without needing for the receiving harness to signal that
   * it is ready to consume that data.
   */
  private final class InboundObserver implements StreamObserver<BeamFnApi.Elements> {

    private final ConcurrentHashMap<String, SynchronousQueue<Data>> instructionIdToQueue;
    private final ConcurrentHashMap<String, HashSet<Consumer<Data>>> instructionIdToConsumers;

    public InboundObserver(boolean enableQueuing) {
      this.instructionIdToQueue =
          enableQueuing ? new ConcurrentHashMap<String, SynchronousQueue<Data>>() : null;
      this.instructionIdToConsumers = new ConcurrentHashMap<String, HashSet<Consumer<Data>>>();
    }

    private void registerConsumer(
        String instructionId, Consumer<BeamFnApi.Elements.Data> consumer) {
      this.instructionIdToConsumers.putIfAbsent(instructionId, new HashSet<Consumer<Data>>());
      this.instructionIdToConsumers.get(instructionId).add(consumer);
    }

    private boolean isFinalizationData(BeamFnApi.Elements.Data data) {
      return data.getData().isEmpty();
    }

    // TODO overall error handling needs an audit and possibly we need to be creative about
    // testing here. Maybe handleData should just throw all exceptions, and catch them
    // and call onError in the calling code?
    private void handleData(BeamFnApi.Elements.Data data) {
      try {
        LogicalEndpoint key = LogicalEndpoint.of(data.getInstructionReference(), data.getTarget());
        CompletableFuture<Consumer<BeamFnApi.Elements.Data>> consumer = receiverFuture(key);
        if (!consumer.isDone()) {
          LOG.debug(
              "Received data for key {} without consumer ready. "
                  + "Waiting for consumer to be registered.",
              key);
        }
        consumer.get().accept(data);
        if (isFinalizationData(data)) {
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

    private boolean IsQueueingEnabled() {
      return instructionIdToQueue != null;
    }


    // TODO please help reveiw the concurrency aspect here. Questions:
    // - Safe use of ConcurrentHashMap?
    // - Unnecessary use of ConcurrentHashMap?
    // - Is there some code that can run concurrently now, which was not before?
    @Override
    public void onNext(BeamFnApi.Elements value) {
      for (BeamFnApi.Elements.Data data : value.getDataList()) {
        try {
          if (IsQueueingEnabled()) {
            // TODO Is there a cleaner way do to this?
            // I was trying to avoid always doing multiple lookups, and always creating
            // a new instance on the heap
            SynchronousQueue<Data> queue = instructionIdToQueue.get(data.getInstructionReference());
            if (queue == null) {
              instructionIdToQueue.putIfAbsent(
                  data.getInstructionReference(), new SynchronousQueue<Data>());
              // Lookup the queue again incase there was a race and another thread added one.
              queue = instructionIdToQueue.get(data.getInstructionReference());
            }

            // TODO, how do we make this throw an interrupted exception? And when should we
            // do this?
            // TODO is there any concurrency issue here? Can multiple threads call onNext?
            // What happens when two threads use the SymetricQueue.
            queue.put(data);
          } else {
            handleData(data);
          }

        } catch (Exception e) {
          outboundObserver.onError(e);
        }
      }
    }

    public void drainAndBlock(String instructionId) {
      int numConsumersComplete = 0;
      SynchronousQueue<Data> queue = instructionIdToQueue.get(instructionId);
      if (queue == null) {
        instructionIdToQueue.putIfAbsent(instructionId, new SynchronousQueue<Data>());
        // Lookup the queue again incase there was a race and another thread added one.
        queue = instructionIdToQueue.get(instructionId);
      }

      while (true) {
        try {
          BeamFnApi.Elements.Data data = null;
          data = queue.poll(50, TimeUnit.MILLISECONDS);
          if (data == null) {
            continue;
          } else {
            handleData(data);
          }

          if (isFinalizationData(data)) {
            // Empty data indicates that the stream of data for this instruction is
            // terminated. Exit once we receive a
            numConsumersComplete++;
            // TODO Assumes we register before we start to block here
            // is this a safe guarantee to rely on?
            // Can we lookup the consumers and check for done status instead of counting?
            if (numConsumersComplete == this.instructionIdToConsumers.get(instructionId).size()) {
              break;
            }
          }
        } catch (Exception e) {
          // Should this be in the loop or outside?
          outboundObserver.onError(e);
        }
      }
      // Remove this queue, as it is no longer needed.
      instructionIdToQueue.remove(instructionId);
      instructionIdToConsumers.remove(instructionId);
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
