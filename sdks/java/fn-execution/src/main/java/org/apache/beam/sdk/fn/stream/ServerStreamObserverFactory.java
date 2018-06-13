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
package org.apache.beam.sdk.fn.stream;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * A {@link StreamObserver} factory that wraps provided {@link StreamObserver}s making them flow
 * control aware and safe to send data from multiple threads.
 *
 * <p>This is different than the {@link StreamObserverFactory} since {@link CallStreamObserver}s can
 * have their {@link CallStreamObserver#setOnReadyHandler} set directly. This allows us to avoid
 * needing to use a {@link ForwardingClientResponseObserver} to wrap the inbound {@link
 * StreamObserver}.
 */
public abstract class ServerStreamObserverFactory {
  /** Wraps the provided outbound {@link StreamObserver}. */
  public abstract <T> StreamObserver<T> from(StreamObserver<T> outboundObserver);

  /**
   * Wraps provided outbound {@link StreamObserver}s with a thread-safe and flow control aware
   * {@link DirectStreamObserver}.
   */
  public static ServerStreamObserverFactory direct() {
    return new Direct();
  }

  private static class Direct extends ServerStreamObserverFactory {
    @Override
    public <T> StreamObserver<T> from(StreamObserver<T> outboundObserver) {
      AdvancingPhaser phaser = new AdvancingPhaser(1);
      CallStreamObserver<T> outboundCallStreamObserver = (CallStreamObserver<T>) outboundObserver;
      outboundCallStreamObserver.setOnReadyHandler(phaser::arrive);
      return new DirectStreamObserver<>(phaser, outboundCallStreamObserver);
    }
  }
}
