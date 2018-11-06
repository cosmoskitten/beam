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
import java.util.HashMap;
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
import org.apache.beam.sdk.util.WindowedValue;
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


  // TODO ajamato. WHy is this constructed twice?
  public BeamFnDataGrpcMultiplexer(
      @Nullable Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      OutboundObserverFactory outboundObserverFactory,
      OutboundObserverFactory.BasicFactory<Elements, Elements> baseOutboundObserverFactory) {
    this.apiServiceDescriptor = apiServiceDescriptor;
    this.consumers = new ConcurrentHashMap<>();
    this.inboundObserver = new InboundObserver();
    this.outboundObserver =
        outboundObserverFactory.outboundObserverFor(baseOutboundObserverFactory, inboundObserver);
    // TODO implement process bundle ids to queues.

    LOG.info("ajamato BeamFnDataGrpcMultiplexer: '" + this.apiServiceDescriptor +
        "' this.apiServiceDescriptor " + System.identityHashCode(this.apiServiceDescriptor) +
        " this " + System.identityHashCode(this));
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
    // Ajamato this is whwere we get which consumer/BeamFnDataInboundObserver to call.
    return consumers.computeIfAbsent(
        endpoint, (LogicalEndpoint unused) -> new CompletableFuture<>());
  }

  public void registerConsumer(
      LogicalEndpoint inputLocation, Consumer<BeamFnApi.Elements.Data> dataBytesReceiver) {
    receiverFuture(inputLocation).complete(dataBytesReceiver);
  }

  @VisibleForTesting
  boolean hasConsumer(LogicalEndpoint outputLocation) {
    return consumers.containsKey(outputLocation);
  }

  @Override
  public void close() {
    // TODO ajamato, this does not seem to be called
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

    //private final SynchronousQueue<BeamFnApi.Elements.Data> queue;
    private final ConcurrentHashMap<String, SynchronousQueue<Data>> instructionIdToQueue;

    public InboundObserver() {
      //this.queue = new SynchronousQueue<>();
      instructionIdToQueue = new ConcurrentHashMap<String, SynchronousQueue<Data>>();
    }

    private void handleData(BeamFnApi.Elements.Data data) {
      try {
        LogicalEndpoint key =
            LogicalEndpoint.of(data.getInstructionReference(), data.getTarget());
        CompletableFuture<Consumer<BeamFnApi.Elements.Data>> consumer = receiverFuture(key);
        if (!consumer.isDone()) {
          LOG.debug(
              "Received data for key {} without consumer ready. "
                  + "Waiting for consumer to be registered.",
              key);
        }
        consumer.get().accept(data);
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

    /*
    @Override
    public void onNext(BeamFnApi.Elements value) {
      for (BeamFnApi.Elements.Data data : value.getDataList()) {
        handleData(data);
      }
    }*/

    // TODO concurrency issues?
    // Need to protect all the instance vars here?

    // TODO ajamato(*)(2) instead of calling accept, put messages
    // on a queue, by adding in some key to do the consumer lookup.
    // OR put the data in directly and do the lookup after dequeuing?
    @Override
    public void onNext(BeamFnApi.Elements value) {
      for (BeamFnApi.Elements.Data data : value.getDataList()) {
        // TODO, do I need a concurrent hashmap?
        instructionIdToQueue.putIfAbsent(
            data.getInstructionReference(), new SynchronousQueue<Data>());
        SynchronousQueue<Data> queue = instructionIdToQueue.get(data.getInstructionReference());
        try {
          LOG.info("ajamato ENQUEUE for " + data.getInstructionReference() +
              " queue " + System.identityHashCode(queue) +
              " this " + System.identityHashCode(this));
          queue.put(data);
        } catch (Exception e) {
          // pass TODO.
          LOG.error("ajamato SOMETHING WENT WRONG", e);
          // call onerror?
          outboundObserver.onError(e);
        }
      }
    }

    // TODO ajamato(*)(1) Add a function here to drain the queue and call it in
    // ProcessBundleHandler.
    public void drainAndBlock(String instructionId) {
      // TODO probably a way to do this without using the heap every time.
      instructionIdToQueue.putIfAbsent(instructionId, new SynchronousQueue<Data>());
      SynchronousQueue<Data> queue = instructionIdToQueue.get(instructionId);
      // Can we ask all the consumers if they are all done?
      LOG.error("ajamato drainAndBlock using queue: " + instructionId + " queue " +
          System.identityHashCode(queue) + " this " + System.identityHashCode(this));
      while (true) {
        try {
          BeamFnApi.Elements.Data data = null;
          data = queue.poll(50, TimeUnit.MILLISECONDS);
          if (data == null) {
            LOG.error("ajamato drainAndBlock no data, continue: " + instructionId +
                " queue " + System.identityHashCode(queue) + " this " + System.identityHashCode(this));
            continue;
          } else if (data.getData().isEmpty()) {
            // Empty data indicates that the stream of data for this instruction is
            // terminated.
            LOG.error("ajamato drainAndBlock DONE " + instructionId);
            break;
          } else {
            LOG.error("ajamato DEQUEUED for " + data.getInstructionReference() + " using id: " +
                instructionId + " queue " + System.identityHashCode(queue) + " this " +
                System.identityHashCode(this));
            handleData(data);
          }
        } catch (Exception e) {
          // pass TODO.
          LOG.error("ajamato SOMETHING WENT WRONG ", e);
          // TOOD, should we break here?
          outboundObserver.onError(e);
        }
      }
      LOG.error("ajamato drainAndBlock RETURNING " + instructionId);
      // Remove this queue, as it is no longer needed.
      instructionIdToQueue.remove(instructionId);
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
