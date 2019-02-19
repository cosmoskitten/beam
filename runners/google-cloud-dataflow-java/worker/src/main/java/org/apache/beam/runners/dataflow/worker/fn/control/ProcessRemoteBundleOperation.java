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
package org.apache.beam.runners.dataflow.worker.fn.control;

import java.io.Closeable;
import java.util.*;
import java.util.Map;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReceivingOperation;
import org.apache.beam.runners.fnexecution.control.*;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link org.apache.beam.runners.dataflow.worker.util.common.worker.Operation} is responsible
 * for communicating with the SDK harness and asking it to process a bundle of work. This operation
 * requests a {@link org.apache.beam.runners.fnexecution.control.RemoteBundle}, sends elements to
 * SDK and receive processed results from SDK, passing these elements downstream.
 */
public class ProcessRemoteBundleOperation<InputT> extends ReceivingOperation {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessRemoteBundleOperation.class);
  private final StageBundleFactory stageBundleFactory;
  private static final OutputReceiver[] EMPTY_RECEIVER_ARRAY = new OutputReceiver[0];
  private final Map<String, OutputReceiver> outputReceiverMap;
  private final OutputReceiverFactory receiverFactory =
      new OutputReceiverFactory() {
        @Override
        public FnDataReceiver<?> create(String pCollectionId) {
          return receivedElement -> receive(pCollectionId, receivedElement);
        }
      };
  private final StateRequestHandler stateRequestHandler;
  private final BundleProgressHandler progressHandler;
  private RemoteBundle remoteBundle;
  private ExecutableStage executableStage;

  private final TimerReceiver timerReceiver;

  public ProcessRemoteBundleOperation(
      ExecutableStage executableStage,
      DataflowOperationContext operationContext,
      StageBundleFactory stageBundleFactory,
      Map<String, OutputReceiver> outputReceiverMap,
      TimerReceiver timerReceiver,
      StateRequestHandlerImpl stateRequestHandler) {
    super(EMPTY_RECEIVER_ARRAY, operationContext);

    this.timerReceiver = timerReceiver;
    this.stageBundleFactory = stageBundleFactory;
    this.stateRequestHandler = stateRequestHandler;
    this.progressHandler = BundleProgressHandler.ignored();
    this.executableStage = executableStage;
    this.outputReceiverMap = outputReceiverMap;
  }

  @Override
  public void start() throws Exception {
    try (Closeable scope = context.enterStart()) {
      super.start();
      try {
        remoteBundle =
            stageBundleFactory.getBundle(receiverFactory, stateRequestHandler, progressHandler);
      } catch (Exception e) {
        throw new RuntimeException("Failed to start remote bundle", e);
      }
    }
  }

  @Override
  public void process(Object inputElement) throws Exception {
    LOG.debug("Sending element: {}", inputElement);
    String mainInputPCollectionId = executableStage.getInputPCollection().getId();
    FnDataReceiver<WindowedValue<?>> mainInputReceiver =
        remoteBundle.getInputReceivers().get(mainInputPCollectionId);

    try (Closeable scope = context.enterProcess()) {
      mainInputReceiver.accept((WindowedValue<InputT>) inputElement);
    } catch (Exception e) {
      String err =
          String.format(
              "Could not process element %s to receiver %s for pcollection %s with error %s",
              inputElement.toString(), mainInputReceiver.toString(), mainInputPCollectionId, e);
      LOG.error(err);
      throw new RuntimeException(err, e);
    }
  }

  @Override
  public void finish() throws Exception {
    try (Closeable scope = context.enterFinish()) {
      try {
        // close blocks until all results are received
        remoteBundle.close();
      } catch (Exception e) {
        throw new RuntimeException("Failed to finish remote bundle", e);
      }

      // Wait until all timer elements are received and scheduled, then fire the timers.
      timerReceiver.finish();
    }
  }

  private void receive(String pCollectionId, Object receivedElement) throws Exception {
    LOG.debug("Received element {} for pcollection {}", receivedElement, pCollectionId);
    if (!timerReceiver.receive(pCollectionId, receivedElement)) {
      outputReceiverMap.get(pCollectionId).process((WindowedValue<?>) receivedElement);
    }
  }
}
