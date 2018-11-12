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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements.Data;
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
 * A {@link BeamFnDataClient} that queues elements so that they can be consumed and processed.
 * In the thread which calls drainAndBlock.
 */
public class QueuingBeamFnDataGrpcClient implements BeamFnDataClient {

  private static final Logger LOG = LoggerFactory.getLogger(QueuingBeamFnDataGrpcClient.class);

  private final BeamFnDataClient mainClient;
  private final SynchronousQueue<ConsumerAndData> queue;
  private final HashSet<InboundDataClient> idcs;

  public QueuingBeamFnDataGrpcClient(BeamFnDataClient mainClient) {
    this.mainClient = mainClient;
    this.queue = new SynchronousQueue<>();
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

  public void drainAndBlock() throws Exception {
    int numConsumersComplete = 0;

    while (true) {
      try {
        ConsumerAndData tuple = null;
        tuple = queue.poll(50, TimeUnit.MILLISECONDS);
        // TODO should we implement a timeout logic here? What happens if the
        // putting thread throws an exception?
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
      } catch (Exception e) {
        // TODO is there some way to cancel the thread putting on the queue if we throw an
        // exception here?
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

    public QueueingFnDataReceiver(FnDataReceiver<WindowedValue<T>> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void accept(WindowedValue<T> value) throws Exception {
      try {
        queue.put(new ConsumerAndData(this.consumer, value));
      } catch (Exception e) {
        // TODO notify the consumer of the error?
        // Interrupt the queue? Notify the consumers?
        throw e;
      }
    }
  }
}

class ConsumerAndData<T> {
  public FnDataReceiver<WindowedValue<T>> consumer;
  public WindowedValue<T>  data;

  public ConsumerAndData(
      FnDataReceiver<WindowedValue<T>> receiver, WindowedValue<T>  data) {
    this.consumer = receiver;
    this.data = data;
  }
}