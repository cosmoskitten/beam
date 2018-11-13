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
package org.apache.beam.fn.harness.data;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BeamFnDataClient} that queues elements so that they can be consumed and processed. In
 * the thread which calls drainAndBlock.
 */
public class QueueingBeamFnDataClient implements BeamFnDataClient {

  private static final Logger LOG = LoggerFactory.getLogger(QueueingBeamFnDataClient.class);

  private final BeamFnDataClient mainClient;
  private final SynchronousQueue<ConsumerAndData> queue;
  private final ConcurrentHashMap<InboundDataClient, Object> idcs;

  public QueueingBeamFnDataClient(BeamFnDataClient mainClient) {
    this.mainClient = mainClient;
    this.queue = new SynchronousQueue<>();
    this.idcs = new ConcurrentHashMap<>();
  }

  /**
   * Registers the following inbound stream consumer for the provided instruction id and target.
   *
   * <p>The provided coder is used to decode elements on the inbound stream. The decoded elements
   * are passed to the provided consumer. Any failure during decoding or processing of the element
   * will complete the returned future exceptionally. On successful termination of the stream
   * (signaled by an empty data block), the returned future is completed successfully.
   */
  @Override
  public <T> InboundDataClient receive(
      ApiServiceDescriptor apiServiceDescriptor,
      LogicalEndpoint inputLocation,
      Coder<WindowedValue<T>> coder,
      FnDataReceiver<WindowedValue<T>> consumer) {
    LOG.debug(
        "Registering consumer for instruction {} and target {}",
        inputLocation.getInstructionId(),
        inputLocation.getTarget());

    QueueingFnDataReceiver<T> queueingConsumer = new QueueingFnDataReceiver<T>(consumer);
    InboundDataClient idc =
        this.mainClient.receive(apiServiceDescriptor, inputLocation, coder, queueingConsumer);
    queueingConsumer.idc = idc;
    this.idcs.computeIfAbsent(idc, (InboundDataClient idcToStore) -> idcToStore);
    return idc;
  }

  // Returns true if all the InboundDataClients have finished or cancelled.
  private boolean allDone() {
    boolean done = true;
    for (InboundDataClient idc : idcs.keySet()) {
      done &= idc.isDone();
    }
    return done;
  }

  /**
   * Drains the internal queue of this class, by waiting for all WindowValues to be passed to thier
   * consumers. The thread which wishes to process() the elements should call this method, as this
   * will cause the consumers to invoke element processing. All receive() and send() calls must be
   * made prior to calling drainAndBlock, in order to properly terminate.
   */
  public void drainAndBlock() throws Exception {
    while (true) {
      try {
        ConsumerAndData tuple = null;
        tuple = queue.poll(2000, TimeUnit.MILLISECONDS);
        if (tuple == null) {
          continue;
        } else {
          // Forward to the consumers who cares about this data.
          tuple.consumer.accept(tuple.data);
        }

        // TODO is there a possible race here? Leading to data loss?
        // Can this be set to done, but more elements come in after?
        if (allDone()) {
          break;
        }
      } catch (Exception e) {
        LOG.error("Client failed to dequeue and process WindowValue",  e);
        for (InboundDataClient idc : idcs.keySet()) {
          idc.fail(e);
        }
        throw e;
      }
    }
  }

  /**
   * Creates a {@link CloseableFnDataReceiver} using the provided instruction id and target.
   *
   * <p>The provided coder is used to encode elements on the outbound stream.
   *
   * <p>Closing the returned receiver signals the end of the stream.
   *
   * <p>The returned closeable receiver is not thread safe.
   */
  @Override
  public <T> CloseableFnDataReceiver<WindowedValue<T>> send(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      LogicalEndpoint outputLocation,
      Coder<WindowedValue<T>> coder) {
    LOG.debug(
        "Creating output consumer for instruction {} and target {}",
        outputLocation.getInstructionId(),
        outputLocation.getTarget());
    return this.mainClient.send(apiServiceDescriptor, outputLocation, coder);
  }

  public class QueueingFnDataReceiver<T> implements FnDataReceiver<WindowedValue<T>> {
    private final FnDataReceiver<WindowedValue<T>> consumer;
    public InboundDataClient idc;

    public QueueingFnDataReceiver(FnDataReceiver<WindowedValue<T>> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void accept(WindowedValue<T> value) throws Exception {
      try{
        ConsumerAndData offering = new ConsumerAndData(this.consumer, value);
        while (!queue.offer(offering, 2000, TimeUnit.MILLISECONDS)) {
          if (idc.isDone()) {
            // If it was cancelled by the consuming side of the queue.
            break;
          }
        }
      } catch (Exception e) {
        LOG.error("Failed to insert WindowValue into the queue", e);
        idc.fail(e);
        throw e;
      }
    }
  }

  static class ConsumerAndData<T> {
    public FnDataReceiver<WindowedValue<T>> consumer;
    public WindowedValue<T> data;

    public ConsumerAndData(FnDataReceiver<WindowedValue<T>> receiver, WindowedValue<T> data) {
      this.consumer = receiver;
      this.data = data;
    }
  }
}