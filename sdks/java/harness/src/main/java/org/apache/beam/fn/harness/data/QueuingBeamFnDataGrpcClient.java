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

import java.util.HashSet;
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
public class QueuingBeamFnDataGrpcClient implements BeamFnDataClient {

  private static final Logger LOG = LoggerFactory.getLogger(QueuingBeamFnDataGrpcClient.class);

  private final BeamFnDataClient mainClient;
  private final SynchronousQueue<ConsumerAndData> queue;
  private final HashSet<InboundDataClient> idcs;

  public QueuingBeamFnDataGrpcClient(BeamFnDataClient mainClient) {
    this.mainClient = mainClient;
    this.queue = new SynchronousQueue<>();
    // TODO does this need to be a concurrent hash map (set doesn't seem to exist).
    this.idcs = new HashSet<InboundDataClient>();
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

    QueueingFnDataReceiver<T> newConsumer = new QueueingFnDataReceiver<T>(consumer);
    InboundDataClient idc =
        this.mainClient.receive(apiServiceDescriptor, inputLocation, coder, newConsumer);
    newConsumer.idc = idc;
    this.idcs.add(idc);
    return idc;
  }

  private boolean AllDone() {
    boolean allDone = true;
    for (InboundDataClient idc : idcs) {
      allDone &= idc.isDone();
    }
    return allDone;
  }

  /**
   * Drains the internal queue of this class, by waiting for all WindowValues to be passed to thier
   * consumers. The thread which wishes to process() the elements should call this method, as this
   * will cause the consumers to invoke element processing. All receive() and send() calls must be
   * made prior to calling drainAndBlock, in order to properly terminate.
   */
  public void drainAndBlock() throws Exception {
    // Note: We just throw the exception here
    // TODO review the error handling here
    while (true) {
      ConsumerAndData tuple = null;
      tuple = queue.poll(50, TimeUnit.MILLISECONDS);
      // TODO should we implement a timeout logic here? What happens if the
      // putting thread throws an exception? Can we assume the InboundDataClient will be marked
      // done?
      if (tuple == null) {
        continue;
      } else {
        // Forward to the consumers who cares about this data.
        tuple.consumer.accept(tuple.data);
      }

      // TODO is there a possible race here? Leading to data loss?
      // Can this be set to done, but more elements come in after?
      if (AllDone()) {
        break;
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
      // Note: We just throw the exception here
      // TODO please review this error handling.
      ConsumerAndData offering = new ConsumerAndData(this.consumer, value);
      while (!queue.offer(offering, 50, TimeUnit.MILLISECONDS)) {
        if (idc.isDone()) {
          // Discard the element.
          // TODO please review this error handling case
          break;
        }
      }
    }
  }
}

class ConsumerAndData<T> {
  public FnDataReceiver<WindowedValue<T>> consumer;
  public WindowedValue<T> data;

  public ConsumerAndData(FnDataReceiver<WindowedValue<T>> receiver, WindowedValue<T> data) {
    this.consumer = receiver;
    this.data = data;
  }
}
