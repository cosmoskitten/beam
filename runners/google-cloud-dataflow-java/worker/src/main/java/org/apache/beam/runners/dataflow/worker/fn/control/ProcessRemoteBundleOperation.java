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

import com.google.common.collect.Iterables;
import java.io.Closeable;
import java.util.EnumMap;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey.TypeCase;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReceivingOperation;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessRemoteBundleOperation<InputT> extends ReceivingOperation {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessRemoteBundleOperation.class);
  private final StageBundleFactory stageBundleFactory;
  private RemoteBundle remoteBundle;
  private StateRequestHandler stateRequestHandler;
  private BundleProgressHandler progressHandler;

  public ProcessRemoteBundleOperation(
      OperationContext context, StageBundleFactory stageBundleFactory, OutputReceiver[] receivers) {
    super(receivers, context);
    this.stageBundleFactory = stageBundleFactory;
    EnumMap<TypeCase, StateRequestHandler> handlerMap = new EnumMap<>(TypeCase.class);
    stateRequestHandler = StateRequestHandlers.delegateBasedUponType(handlerMap);
    progressHandler = BundleProgressHandler.unsupported();
  }

  @Override
  public void start() throws Exception {
    try (Closeable scope = context.enterStart()) {
      super.start();
      OutputReceiverFactory receiverFactory =
          new OutputReceiverFactory() {
            @Override
            public FnDataReceiver<?> create(String pCollectionId) {
              return receivedElement -> {
                for (OutputReceiver receiver : receivers) {
                  LOG.info("[BOYUANZ LOG]: consume element {}", receivedElement);
                  receiver.process((WindowedValue<?>) receivedElement);
                }
              };
            }
          };

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
    LOG.info(String.format("[BOYUANZ LOG] Sending value: %s", inputElement));
    try (Closeable scope = context.enterProcess()) {
      Iterables.getOnlyElement(remoteBundle.getInputReceivers().values())
          .accept((WindowedValue<InputT>) inputElement);
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
    }
  }
}
