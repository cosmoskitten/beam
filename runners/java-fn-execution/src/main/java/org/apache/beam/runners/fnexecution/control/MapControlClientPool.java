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
package org.apache.beam.runners.fnexecution.control;

import com.google.common.collect.Maps;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.GuardedBy;

/**
 * A {@link ControlClientPool} backed by a client map. It is expected that a given client id will be
 * requested at most once.
 */
public class MapControlClientPool implements ControlClientPool {

  /** Creates a {@link MapControlClientPool} with an unspecified timeout. */
  public static MapControlClientPool create() {
    return withTimeout(Duration.ofSeconds(Long.MAX_VALUE));
  }

  /**
   * Creates a {@link MapControlClientPool} with the given timeout. Timeouts only apply to source
   * requests.
   */
  public static MapControlClientPool withTimeout(Duration timeout) {
    return new MapControlClientPool(timeout);
  }

  private final Duration timeout;
  private final Object lock = new Object();

  @GuardedBy("lock")
  private final Map<String, CompletableFuture<InstructionRequestHandler>> clients =
      Maps.newHashMap();

  private MapControlClientPool(Duration timeout) {
    this.timeout = timeout;
  }

  @Override
  public ControlClientSource getSource() {
    return this::getClient;
  }

  @Override
  public ControlClientSink getSink() {
    return this::putClient;
  }

  private void putClient(String workerId, InstructionRequestHandler client) {
    synchronized (lock) {
      CompletableFuture<InstructionRequestHandler> future =
          clients.computeIfAbsent(workerId, MapControlClientPool::createClientFuture);
      boolean success = future.complete(client);
      if (!success) {
        throw new IllegalStateException(
            String.format("Control client for worker id %s failed to compete", workerId));
      }
    }
  }

  private InstructionRequestHandler getClient(String workerId)
      throws ExecutionException, InterruptedException, TimeoutException {
    CompletableFuture<InstructionRequestHandler> future;
    synchronized (lock) {
      future = clients.computeIfAbsent(workerId, MapControlClientPool::createClientFuture);
    }
    // TODO: Wire in health checking of clients so requests don't hang.
    future.get(timeout.getSeconds(), TimeUnit.SECONDS);
    InstructionRequestHandler client = future.get();
    synchronized (lock) {
      clients.remove(workerId);
    }
    return client;
  }

  private static CompletableFuture<InstructionRequestHandler> createClientFuture(String unused) {
    return new CompletableFuture<>();
  }
}
